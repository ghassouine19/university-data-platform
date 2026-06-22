# jobs/ingestion/images/extract_images.py
"""
Ingesteur d'IMAGES pour les facultés de l'Université Hassan II Casablanca (UH2C).

RESPONSABILITÉ UNIQUE de ce script :
    Découvrir les images présentes sur les pages d'une (ou plusieurs) faculté(s),
    les télécharger, les valider, et les stocker brutes dans MinIO
    (bucket raw-images-dev). Rien d'autre.

CAS GÉRÉS
----------
    1. Page HTML classique (server-rendered)   → BeautifulSoup extrait directement les <img>
    2. Page React/Vue/SPA (client-rendered)     → détection + tentative de découverte d'API JSON
    3. Page vide / erreur serveur / maintenance → log + skip, ne fait JAMAIS planter tout le run
    4. Image cassée / 404 / contenu non-image   → détectée par les "magic bytes", rejetée proprement
    5. Image dupliquée (même contenu, autre URL) → dédupliquée par hash SHA-256
    6. Redirection serveur (ex: / → /front/index.html) → les chemins relatifs sont résolus
                                                           contre l'URL FINALE après redirection.
    7. Page de blocage / captcha (anti-bot)     → détectée, on n'insiste PAS, on met en pause
                                                   le crawl de cette faculté (voir _looks_blocked).
    8. Image déjà traitée lors d'un run précédent (hier, avant-hier...) → on ne la re-télécharge
                                                   PAS, grâce à un petit cache persistant stocké
                                                   dans MinIO (voir CACHE D'URLS PERSISTANT).

CACHE D'URLS PERSISTANT (pourquoi et comment)
----------------------------------------------
    Avant ce fix : chaque run repartait de zéro (seen_hashes vide), donc une image déjà
    téléchargée hier était RE-téléchargée aujourd'hui (gaspillage réseau, et c'est exactement
    le genre de trafic répété qui fait declencher un captcha / un rate-limit côté serveur).

    Avec ce fix : pour chaque faculté, on garde un petit fichier JSON dans MinIO
    ("{faculty}/_cache/processed_urls.json") qui mappe chaque URL d'image déjà vue à son
    résultat (uploaded / duplicate / rejected_*). Au démarrage d'un run, on charge ce cache.
    Si une URL d'image y est déjà présente avec un statut "définitif", on la SKIP sans la
    re-télécharger. Le cache est mis à jour et re-sauvegardé à la fin du run.

    Seules les URLs en erreur réseau (status="error") ne sont PAS mises en cache, pour
    qu'on retente automatiquement au prochain run (erreur probablement temporaire).
"""

import argparse
import hashlib
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from jobs.common.config import settings
from jobs.common.logger import logger
from jobs.common.minio_client import MinioStorageClient
from jobs.common.exceptions import (
    DataLakeConnectionError,
    ScrapingExtractionError,
)

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION (valeurs par défaut — surchargeables via --delay / --max-pages)
# ═════════════════════════════════════════════════════════════════════════════

MAX_CRAWL_DEPTH = 4
MAX_PAGES_PER_FACULTY = 350          # défaut, surchargeable via --max-pages
REQUEST_TIMEOUT_SECONDS = 15
IMAGE_DOWNLOAD_TIMEOUT_SECONDS = 20
REQUEST_DELAY_SECONDS = 1.2          # défaut, surchargeable via --delay
MAX_IMAGE_SIZE_BYTES = 25 * 1024 * 1024
MAX_RETRIES = 2

# Pause "de sécurité" quand on détecte une page de blocage / captcha sur une
# faculté. On ne continue PAS à insister, on laisse le serveur respirer.
BLOCKED_COOLDOWN_SECONDS = 60

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".svg", ".ico", ".bmp", ".tiff", ".tif",
}

IMAGE_MAGIC_BYTES = {
    b"\xff\xd8\xff": "jpeg",
    b"\x89PNG\r\n\x1a\n": "png",
    b"GIF87a": "gif",
    b"GIF89a": "gif",
    b"RIFF": "webp",
    b"BM": "bmp",
    b"\x00\x00\x01\x00": "ico",
    b"II*\x00": "tiff",
    b"MM\x00*": "tiff",
}
SVG_SIGNATURE_HINTS = (b"<svg", b"<?xml")

# URLs d'images à ignorer COMPLÈTEMENT (jamais téléchargées, même pas comptées
# dans "images_found"). Typiquement des images de CAPTCHA générées à la volée
# par un formulaire (sécurité anti-bot) : leur contenu change à chaque
# chargement de page par conception, donc elles n'ont aucune valeur comme
# "contenu" et ne peuvent de toute façon jamais être dédupliquées par hash —
# le seul fix sensé est de ne jamais les récupérer.
IGNORED_IMAGE_URL_KEYWORDS = (
    "captcha",
    "recaptcha",
    "hcaptcha",
    "verification-code",
    "security-code",
)

# Mots-clés simples indiquant qu'on est tombé sur une page de blocage anti-bot
# (captcha, rate-limit déguisé en page HTML, pare-feu applicatif, etc.).
# Heuristique volontairement simple : on ne cherche PAS à contourner ces
# protections, juste à les détecter pour arrêter d'insister et ne pas
# aggraver un éventuel blocage en cours.
BLOCKED_PAGE_KEYWORDS = (
    "captcha",
    "recaptcha",
    "are you a human",
    "are you human",
    "verify you are a human",
    "unusual traffic",
    "access denied",
    "attention required",
    "checking your browser",
    "ddos protection",
)

UH2C_FACULTY_SEEDS = {
    "univh2c-main": "https://www.univh2c.ma/",
    "fsac":         "https://fsac.univh2c.ma/",
    "fsbm":         "https://www.fsb.univh2c.ma/",
    "fstm":         "https://www.fstm.ac.ma/",
    "flsh":         "https://flsh-uh2c.ac.ma/",
    "fsjes":        "https://www.fdc.ma/",
    "fsjesas":      "https://fsjesas.univh2c.ma/",
    "enset":        "https://enset-media.ac.ma/",
    "encg":         "https://www.encgcasa.ma/",
    "ensa":         "http://www.ensam-casa.ma/",
}

# Statuts "définitifs" qu'on a le droit de mettre en cache et donc de ne plus
# jamais re-tenter au run suivant. "error" est volontairement absent : une
# erreur réseau est probablement temporaire, on veut pouvoir réessayer demain.
CACHEABLE_STATUSES = {"uploaded", "duplicate", "rejected_invalid", "rejected_too_large"}


@dataclass
class FacultyImageStats:
    faculty: str
    pages_visited: int = 0
    pages_skipped_empty: int = 0
    pages_skipped_spa_no_api: int = 0
    pages_skipped_blocked: int = 0
    images_found: int = 0
    images_uploaded: int = 0
    images_duplicate: int = 0
    images_rejected_invalid: int = 0
    images_rejected_too_large: int = 0
    images_skipped_cached: int = 0   # déjà traitées lors d'un run précédent
    images_ignored_filtered: int = 0  # ex: CAPTCHA dynamique, jamais téléchargées
    errors: int = 0


def _sleep_with_jitter(base_delay: float) -> None:
    """
    Dort `base_delay` secondes + un petit aléa (jusqu'à +40%).
    Un délai parfaitement constant entre deux requêtes est un signal facile
    à repérer pour un système anti-bot ; un peu de variation rend le trafic
    moins mécanique, sans pour autant ralentir le run de façon significative.
    """
    time.sleep(base_delay + random.uniform(0, base_delay * 0.4))


def _looks_blocked(html_content: str) -> bool:
    """
    Détecte (heuristique simple, pas infaillible) une page de blocage / captcha.
    On ne regarde que le début du HTML : si une protection anti-bot s'est
    déclenchée, c'est généralement visible dès les premiers caractères.
    """
    lowered = html_content[:5000].lower()
    return any(keyword in lowered for keyword in BLOCKED_PAGE_KEYWORDS)


def _is_ignored_image_url(url: str) -> bool:
    """True si l'URL correspond à une image qu'on ne veut JAMAIS télécharger
    (ex: CAPTCHA dynamique). Voir IGNORED_IMAGE_URL_KEYWORDS ci-dessus."""
    lowered = url.lower()
    return any(keyword in lowered for keyword in IGNORED_IMAGE_URL_KEYWORDS)


def _filter_ignored_urls(urls: set[str]) -> tuple[set[str], int]:
    """Retire du set les URLs ignorées. Retourne (urls_gardées, nb_filtrées)."""
    kept = {u for u in urls if not _is_ignored_image_url(u)}
    return kept, len(urls) - len(kept)


def _get_base_domain(url: str) -> str:
    return urlparse(url).netloc


def _is_same_domain(url: str, base_domain: str) -> bool:
    domain = urlparse(url).netloc
    return domain == base_domain or domain.endswith("." + base_domain)


def _has_image_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in IMAGE_EXTENSIONS)


def _url_to_filename(url: str, content_hash: str) -> str:
    path = urlparse(url).path
    original_name = path.split("/")[-1].split("?")[0] or "image"
    original_name = re.sub(r"[^a-zA-Z0-9\-_.]", "-", original_name)

    if "." not in original_name:
        original_name += ".bin"

    name_part, _, ext_part = original_name.rpartition(".")
    short_hash = content_hash[:10]
    return f"{name_part}__{short_hash}.{ext_part}"


def _detect_page_type(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "lxml")
    body = soup.find("body")

    if not body:
        return "empty"

    body_text = body.get_text(separator=" ", strip=True)
    if len(body_text) < 100:
        return "empty"

    spa_mounts = body.find_all(id=re.compile(r"^(root|app|__next|__nuxt)$"))
    for mount in spa_mounts:
        if not mount.get_text(strip=True):
            return "spa"

    return "html"


def _find_hidden_image_api(base_url: str, html_content: str) -> Optional[str]:
    domain = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
    soup = BeautifulSoup(html_content, "lxml")

    api_pattern = re.compile(r'["\'](/api/v?\d*/[a-zA-Z/]+)["\']')
    for script in soup.find_all("script"):
        if script.string:
            match = api_pattern.search(script.string)
            if match:
                return urljoin(domain, match.group(1))

    common_paths = ["/wp-json/wp/v2/media", "/api/gallery", "/api/images"]
    for path in common_paths:
        candidate = urljoin(domain, path)
        try:
            resp = requests.get(candidate, headers=HEADERS, timeout=5)
            if resp.status_code == 200 and "json" in resp.headers.get("Content-Type", ""):
                return candidate
        except requests.RequestException:
            continue
    return None


def _detect_real_image_type(content: bytes) -> Optional[str]:
    if not content or len(content) < 8:
        return None

    head = content[:200].lstrip()
    if any(head.startswith(sig) for sig in SVG_SIGNATURE_HINTS) and b"<svg" in content[:500]:
        return "svg"

    for signature, image_type in IMAGE_MAGIC_BYTES.items():
        if content.startswith(signature):
            if image_type == "webp":
                if content[8:12] == b"WEBP":
                    return "webp"
                continue
            return image_type

    return None


def _fetch_with_retry(url: str, timeout: int, stream: bool = False) -> Optional[requests.Response]:
    """
    NOTE: requests suit les redirections par défaut (allow_redirects=True) et
    response.url contient l'URL FINALE après toute redirection. C'est cette
    URL finale qui doit être utilisée comme base pour résoudre les chemins
    relatifs trouvés dans le HTML — pas l'URL initialement demandée.
    """
    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            response = requests.get(
                url, headers=HEADERS, timeout=timeout, stream=stream, allow_redirects=True,
            )

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 10))
                logger.warning(f"[429] Rate limited sur {url} — attente {wait}s")
                time.sleep(wait)
                attempt += 1
                continue

            if 400 <= response.status_code < 500:
                logger.warning(f"[HTTP {response.status_code}] {url} — pas de retry (erreur client)")
                return None

            if response.status_code >= 500:
                if attempt < MAX_RETRIES:
                    logger.warning(f"[HTTP {response.status_code}] {url} — retry ({attempt+1}/{MAX_RETRIES})")
                    time.sleep(4)
                    attempt += 1
                    continue
                logger.error(f"[HTTP {response.status_code}] {url} — abandon après {MAX_RETRIES} tentatives")
                return None

            return response

        except requests.exceptions.ConnectionError as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"[ConnectionError] {url} — retry ({attempt+1}/{MAX_RETRIES}): {e}")
                time.sleep(3)
                attempt += 1
            else:
                logger.error(f"[ConnectionError] {url} — abandon: {e}")
                return None

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES:
                logger.warning(f"[Timeout] {url} — retry ({attempt+1}/{MAX_RETRIES})")
                time.sleep(3)
                attempt += 1
            else:
                logger.error(f"[Timeout] {url} — abandon après {MAX_RETRIES} tentatives")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"[RequestException] {url} — {e}")
            return None

    return None


def _extract_image_urls_from_html(html_content: str, page_url: str) -> set[str]:
    """
    IMPORTANT : page_url DOIT être l'URL finale après redirection (response.url),
    pas l'URL initialement demandée. Sinon les chemins relatifs se résolvent au
    mauvais endroit et toutes les images renvoient 404.
    """
    soup = BeautifulSoup(html_content, "lxml")
    base_domain = _get_base_domain(page_url)
    found_urls: set[str] = set()

    def _maybe_add(raw_url: Optional[str]):
        if not raw_url or raw_url.strip().startswith("data:"):
            return
        absolute = urljoin(page_url, raw_url.strip())
        if _is_same_domain(absolute, base_domain) or _has_image_extension(absolute):
            found_urls.add(absolute)

    for img in soup.find_all("img"):
        _maybe_add(img.get("src"))
        _maybe_add(img.get("data-src"))
        srcset = img.get("srcset")
        if srcset:
            for part in srcset.split(","):
                _maybe_add(part.strip().split(" ")[0])

    for source in soup.find_all("source"):
        srcset = source.get("srcset")
        if srcset:
            for part in srcset.split(","):
                _maybe_add(part.strip().split(" ")[0])

    for link in soup.find_all("link", rel=re.compile(r"icon", re.I)):
        _maybe_add(link.get("href"))

    bg_pattern = re.compile(r'background-image\s*:\s*url\(["\']?([^)"\']+)["\']?\)')
    for tag in soup.find_all(style=True):
        match = bg_pattern.search(tag["style"])
        if match:
            _maybe_add(match.group(1))

    return found_urls


# ═════════════════════════════════════════════════════════════════════════════
# CACHE D'URLS PERSISTANT — survit entre deux runs (aujourd'hui / hier / ...)
# ═════════════════════════════════════════════════════════════════════════════

def _cache_object_name(faculty: str) -> str:
    return f"{faculty}/_cache/processed_urls.json"


def _load_url_cache(minio_client: MinioStorageClient, bucket: str, faculty: str) -> dict:
    """
    Charge le cache JSON {url: {"status": ..., "hash": ..., "last_seen": ...}}
    pour cette faculté. Retourne un dict vide si rien n'existe encore
    (premier run pour cette faculté → comportement normal, pas une erreur).
    """
    try:
        data = minio_client.download_json(bucket, _cache_object_name(faculty))
        if data:
            logger.info(f"[{faculty}] Cache chargé : {len(data)} URLs déjà connues (runs précédents)")
            return data
    except Exception as e:
        logger.warning(f"[{faculty}] Impossible de charger le cache d'URLs, on repart à vide : {e}")
    return {}


def _save_url_cache(minio_client: MinioStorageClient, bucket: str, faculty: str, cache: dict) -> None:
    try:
        minio_client.upload_json(cache, bucket, _cache_object_name(faculty))
    except Exception as e:
        logger.warning(f"[{faculty}] Impossible de sauvegarder le cache d'URLs (sera retenté demain) : {e}")


# Le nom de fichier final embarque toujours le hash court du contenu, voir
# _url_to_filename (ex: "btp__4fbb4b2740.jpg" → hash court "4fbb4b2740").
_HASH_SUFFIX_PATTERN = re.compile(r"__([0-9a-f]{10})\.[A-Za-z0-9]+$")


def _scan_existing_content_hashes(minio_client: MinioStorageClient, bucket: str, faculty: str) -> set[str]:
    """
    Scanne TOUT ce qui existe déjà dans MinIO pour cette faculté — TOUTES les
    dates confondues, y compris les fichiers uploadés avant la mise en place
    du cache d'URLs persistant — et en extrait les hash de contenu déjà
    encodés dans les noms de fichiers.

    POURQUOI : le cache d'URLs ne connaît que ce qu'IL a lui-même traité depuis
    son activation. Il ne "voit" pas les images uploadées par d'anciennes
    versions du script (ex: les dossiers du 18 et 19 juin). Ce scan comble ce
    trou en relisant directement la source de vérité (MinIO) plutôt que de se
    fier uniquement à la mémoire du cache.
    """
    known_hashes: set[str] = set()
    try:
        object_keys = minio_client.list_objects(bucket, prefix=f"{faculty}/")
    except Exception as e:
        logger.warning(f"[{faculty}] Impossible de scanner l'historique MinIO, on continue sans : {e}")
        return known_hashes

    for key in object_keys:
        if "/_cache/" in key:
            continue  # le fichier de cache lui-même n'est pas une image
        match = _HASH_SUFFIX_PATTERN.search(key)
        if match:
            known_hashes.add(match.group(1))

    if known_hashes:
        logger.info(
            f"[{faculty}] Historique MinIO scanné : {len(known_hashes)} images déjà "
            f"stockées détectées (toutes dates confondues, y compris avant le cache)"
        )
    return known_hashes


def _download_and_upload_image(
    image_url: str,
    faculty: str,
    minio_client: MinioStorageClient,
    run_date: str,
    seen_hashes: set[str],
    known_content_hashes: set[str],
    url_cache: dict,
    delay: float,
) -> str:
    # ── Étape 0 : cette URL a-t-elle déjà été traitée lors d'un run précédent ? ──
    # Si oui (et que le résultat était définitif), on ne refait PAS la requête
    # réseau. C'est le coeur du fix "duplicate" + "moins de trafic = moins de
    # risque de captcha".
    cached_entry = url_cache.get(image_url)
    if cached_entry and cached_entry.get("status") in CACHEABLE_STATUSES:
        if cached_entry.get("hash"):
            seen_hashes.add(cached_entry["hash"])
        logger.debug(
            f"[Image] Déjà traitée lors d'un run précédent ({cached_entry['status']}), "
            f"pas de re-téléchargement: {image_url}"
        )
        return "skipped_cached"

    _sleep_with_jitter(delay)

    response = _fetch_with_retry(image_url, timeout=IMAGE_DOWNLOAD_TIMEOUT_SECONDS, stream=True)
    if response is None:
        return "error"  # pas mis en cache : on retentera au prochain run

    content_length = int(response.headers.get("Content-Length", 0))
    if content_length > MAX_IMAGE_SIZE_BYTES:
        logger.warning(f"[Image] Trop volumineuse ({content_length/1024/1024:.1f} MB), skip: {image_url}")
        url_cache[image_url] = {"status": "rejected_too_large", "last_seen": run_date}
        return "rejected_too_large"

    # ── Lecture du corps de la réponse ───────────────────────────────────────
    # FIX: _fetch_with_retry ne protège que la connexion INITIALE. Une fois la
    # réponse obtenue (status 200), la lecture du contenu (iter_content) peut
    # encore timeout/se couper en cours de route (image lente/lourde, réseau
    # instable). Sans ce try/except, une telle erreur remontait jusqu'à
    # _process_faculty et faisait perdre TOUTES les stats déjà accumulées pour
    # la faculté (images déjà uploadées avec succès comptées comme "0 uploadées").
    image_bytes = b""
    try:
        for chunk in response.iter_content(chunk_size=8192):
            image_bytes += chunk
            if len(image_bytes) > MAX_IMAGE_SIZE_BYTES:
                logger.warning(f"[Image] Taille dépassée pendant le téléchargement, abandon: {image_url}")
                url_cache[image_url] = {"status": "rejected_too_large", "last_seen": run_date}
                return "rejected_too_large"
    except requests.exceptions.RequestException as e:
        logger.warning(f"[Image] Coupure pendant le téléchargement (sera retenté au prochain run): {image_url} — {e}")
        return "error"  # pas mis en cache : on retentera ce run-ci ou demain

    real_type = _detect_real_image_type(image_bytes)
    if real_type is None:
        logger.warning(
            f"[Image] Contenu non reconnu comme image valide (probable page d'erreur "
            f"ou redirection déguisée): {image_url}"
        )
        url_cache[image_url] = {"status": "rejected_invalid", "last_seen": run_date}
        return "rejected_invalid"

    content_hash = hashlib.sha256(image_bytes).hexdigest()
    short_hash = content_hash[:10]

    # Doublon si : déjà vu ce contenu DANS ce run (seen_hashes), OU déjà
    # présent dans MinIO depuis n'importe quelle date passée (known_content_hashes,
    # alimenté par le scan de l'historique — voir _scan_existing_content_hashes).
    if content_hash in seen_hashes or short_hash in known_content_hashes:
        logger.debug(f"[Image] Doublon (contenu déjà connu, run actuel ou historique MinIO), skip upload: {image_url}")
        url_cache[image_url] = {"status": "duplicate", "hash": content_hash, "last_seen": run_date}
        return "duplicate"
    seen_hashes.add(content_hash)

    filename = _url_to_filename(image_url, content_hash)
    name_without_ext = filename.rsplit(".", 1)[0]
    final_filename = f"{name_without_ext}.{real_type if real_type != 'jpeg' else 'jpg'}"
    object_name = f"{faculty}/{run_date}/{final_filename}"

    bucket = settings.MINIO_RAW_BUCKET_IMAGES

    if minio_client.object_exists(bucket, object_name):
        logger.debug(f"[Image] Déjà présente en MinIO: {object_name}")
        url_cache[image_url] = {"status": "duplicate", "hash": content_hash, "last_seen": run_date}
        return "duplicate"

    content_type_map = {
        "jpeg": "image/jpeg", "png": "image/png", "gif": "image/gif",
        "webp": "image/webp", "svg": "image/svg+xml", "ico": "image/x-icon",
        "bmp": "image/bmp", "tiff": "image/tiff",
    }

    try:
        minio_client.upload_bytes(
            data=image_bytes,
            bucket=bucket,
            object_name=object_name,
            content_type=content_type_map.get(real_type, "application/octet-stream"),
        )
        logger.success(
            f"[Image] ✓ {faculty} | {real_type} | {len(image_bytes)/1024:.1f} KB "
            f"→ {bucket}/{object_name}"
        )
        url_cache[image_url] = {
            "status": "uploaded",
            "hash": content_hash,
            "object_name": object_name,
            "last_seen": run_date,
        }
        known_content_hashes.add(short_hash)
        return "uploaded"
    except DataLakeConnectionError:
        raise
    except Exception as e:
        logger.error(f"[Image] Échec upload MinIO pour {image_url}: {e}")
        return "error"  # pas mis en cache : on retentera au prochain run


def _process_faculty(
    faculty: str,
    seed_url: str,
    minio_client: MinioStorageClient,
    run_date: str,
    delay: float,
    max_pages: int,
) -> FacultyImageStats:
    stats = FacultyImageStats(faculty=faculty)
    base_domain = _get_base_domain(seed_url)
    visited: set[str] = set()
    queue: list[tuple[str, int]] = [(seed_url, 0)]
    all_image_urls: set[str] = set()

    bucket = settings.MINIO_RAW_BUCKET_IMAGES
    url_cache = _load_url_cache(minio_client, bucket, faculty)
    known_content_hashes = _scan_existing_content_hashes(minio_client, bucket, faculty)

    logger.info(
        f"[{faculty}] Démarrage extraction images | seed={seed_url} | "
        f"delay={delay}s | max_pages={max_pages}"
    )

    while queue:
        url, depth = queue.pop(0)

        if url in visited or stats.pages_visited >= max_pages:
            continue
        visited.add(url)

        response = _fetch_with_retry(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if response is None:
            stats.errors += 1
            continue

        _sleep_with_jitter(delay)
        html_content = response.text

        # ── Détection captcha / page de blocage ─────────────────────────────
        # On n'insiste pas : on saute cette page et on laisse le serveur
        # respirer un peu plus longtemps avant de continuer le crawl.
        if _looks_blocked(html_content):
            logger.critical(
                f"[{faculty}] Page de blocage / captcha détectée sur {url} — "
                f"pause de {BLOCKED_COOLDOWN_SECONDS}s avant de continuer"
            )
            stats.pages_skipped_blocked += 1
            time.sleep(BLOCKED_COOLDOWN_SECONDS)
            continue

        # ── FIX : URL réelle après redirection ──────────────────────────────
        # Ex: "https://fsac.univh2c.ma/" redirige vers
        # "https://fsac.univh2c.ma/front/index.html". Tous les chemins relatifs
        # doivent être résolus contre CETTE URL finale, pas l'URL demandée.
        final_url = response.url
        if final_url != url:
            logger.debug(f"[{faculty}] Redirection détectée : {url} → {final_url}")

        page_type = _detect_page_type(html_content)
        stats.pages_visited += 1

        if page_type == "empty":
            logger.warning(f"[{faculty}] Page vide/maintenance, skip: {url}")
            stats.pages_skipped_empty += 1
            continue

        if page_type == "spa":
            logger.info(f"[{faculty}] Page SPA détectée: {url}")
            api_url = _find_hidden_image_api(final_url, html_content)
            if api_url is None:
                logger.info(f"[{faculty}] Aucune API d'images trouvée pour cette SPA — skip documenté: {url}")
                stats.pages_skipped_spa_no_api += 1
                continue
            api_response = _fetch_with_retry(api_url, timeout=REQUEST_TIMEOUT_SECONDS)
            if api_response:
                try:
                    payload = api_response.json()
                    text_blob = str(payload)
                    found_in_api = set(re.findall(r'https?://[^\s"\']+\.(?:jpg|jpeg|png|gif|webp|svg)', text_blob, re.I))
                    found_in_api, n_ignored = _filter_ignored_urls(found_in_api)
                    stats.images_ignored_filtered += n_ignored
                    all_image_urls.update(found_in_api)
                    logger.info(f"[{faculty}] {len(found_in_api)} URLs d'images extraites de l'API SPA")
                except ValueError:
                    logger.warning(f"[{faculty}] Réponse API non-JSON pour {api_url}")
            continue

        # FIX: on utilise final_url (post-redirection), pas url (pré-redirection)
        page_images = _extract_image_urls_from_html(html_content, final_url)
        page_images, n_ignored = _filter_ignored_urls(page_images)
        stats.images_ignored_filtered += n_ignored
        all_image_urls.update(page_images)
        logger.debug(f"[{faculty}] depth={depth} | {len(page_images)} images trouvées sur {final_url}")

        if depth < MAX_CRAWL_DEPTH:
            soup = BeautifulSoup(html_content, "lxml")
            for tag in soup.find_all("a", href=True):
                href = tag["href"].strip()
                if not href or href.startswith(("#", "mailto:", "javascript:", "tel:")):
                    continue
                # Normalize absolute URL by stripping trailing slash (except for host/root domain)
                absolute = urljoin(final_url, href).split("#")[0]
                parsed = urlparse(absolute)
                if parsed.path == "/":
                    absolute = f"{parsed.scheme}://{parsed.netloc}/"
                else:
                    absolute = absolute.rstrip("/")

                if _is_same_domain(absolute, base_domain) and absolute not in visited:
                    queue.append((absolute, depth + 1))

    stats.images_found = len(all_image_urls)
    logger.info(f"[{faculty}] Crawl terminé : {stats.pages_visited} pages, {stats.images_found} URLs d'images uniques trouvées")

    seen_hashes: set[str] = set()
    try:
        for image_url in all_image_urls:
            try:
                result = _download_and_upload_image(
                    image_url, faculty, minio_client, run_date, seen_hashes, known_content_hashes, url_cache, delay,
                )
            except DataLakeConnectionError:
                # MinIO lui-même est down : pas la peine de continuer à essayer
                # d'uploader, on arrête cette faculté proprement.
                logger.critical(f"[{faculty}] MinIO inaccessible en cours de run — arrêt de cette faculté")
                stats.errors += 1
                break
            except Exception as e:
                # FIX: avant ce fix, une erreur inattendue sur UNE SEULE image
                # (ex: coupure réseau pendant le téléchargement d'une image
                # lourde) remontait jusqu'à run() et faisait perdre TOUTES les
                # stats déjà accumulées pour la faculté entière (images déjà
                # uploadées avec succès comptées comme "0 uploadées"). On isole
                # maintenant chaque image : une erreur ne pénalise QUE cette
                # image, le reste du run continue normalement.
                logger.error(f"[{faculty}] Erreur inattendue sur une image, on continue: {image_url} — {e}")
                stats.errors += 1
                continue

            if result == "uploaded":
                stats.images_uploaded += 1
            elif result == "duplicate":
                stats.images_duplicate += 1
            elif result == "rejected_invalid":
                stats.images_rejected_invalid += 1
            elif result == "rejected_too_large":
                stats.images_rejected_too_large += 1
            elif result == "skipped_cached":
                stats.images_skipped_cached += 1
            else:
                stats.errors += 1
    finally:
        # FIX: sauvegarde du cache en `finally`, pas juste en fin de boucle.
        # Même si quelque chose d'imprévu interrompt le run (Ctrl+C, MinIO down,
        # exception non prévue), tout ce qui a déjà été traité avec succès est
        # acquis pour le prochain run — on ne perd pas le travail déjà fait.
        _save_url_cache(minio_client, bucket, faculty, url_cache)

    logger.success(
        f"[{faculty}] TERMINÉ | uploadées={stats.images_uploaded} | "
        f"doublons={stats.images_duplicate} | invalides={stats.images_rejected_invalid} | "
        f"trop_grandes={stats.images_rejected_too_large} | "
        f"déjà_en_cache={stats.images_skipped_cached} | ignorées={stats.images_ignored_filtered} | "
        f"erreurs={stats.errors}"
    )
    return stats


def run(
    faculties: Optional[list[str]] = None,
    delay: float = REQUEST_DELAY_SECONDS,
    max_pages: int = MAX_PAGES_PER_FACULTY,
    **context,
) -> dict:
    """
    Args:
        faculties: liste de clés UH2C_FACULTY_SEEDS à traiter (None/vide = toutes).
        delay: délai (secondes) entre deux requêtes HTTP, +jitter automatique.
               Augmenter cette valeur si une faculté se met à bloquer/capter.
        max_pages: nombre max de pages crawlées PAR faculté avant d'arrêter.
    """
    run_date = datetime.utcnow().strftime("%Y-%m-%d")

    if faculties is None or len(faculties) == 0:
        target_faculties = list(UH2C_FACULTY_SEEDS.keys())
    else:
        target_faculties = faculties

    unknown = [f for f in target_faculties if f not in UH2C_FACULTY_SEEDS]
    if unknown:
        raise ScrapingExtractionError(
            f"Faculté(s) inconnue(s): {unknown}",
            details=f"Facultés valides: {list(UH2C_FACULTY_SEEDS.keys())}",
        )

    logger.info(
        f"=== Ingestion IMAGES démarrée | Date: {run_date} | "
        f"Facultés ciblées: {target_faculties} | delay={delay}s | max_pages={max_pages} ==="
    )

    try:
        minio_client = MinioStorageClient()
        if not minio_client.check_connection():
            raise DataLakeConnectionError(
                "MinIO inaccessible au démarrage de l'ingestion d'images",
                details=f"Endpoint: {settings.MINIO_ENDPOINT}",
            )
    except Exception as e:
        logger.critical(f"Impossible de se connecter à MinIO: {e}")
        raise

    all_stats: list[FacultyImageStats] = []

    for faculty in target_faculties:
        seed_url = UH2C_FACULTY_SEEDS[faculty]
        try:
            stats = _process_faculty(faculty, seed_url, minio_client, run_date, delay, max_pages)
            all_stats.append(stats)
        except DataLakeConnectionError:
            raise
        except Exception as e:
            logger.error(f"[{faculty}] Erreur inattendue, faculté ignorée: {e}")
            all_stats.append(FacultyImageStats(faculty=faculty, errors=1))

    summary = {
        "run_date": run_date,
        "delay_used": delay,
        "max_pages_used": max_pages,
        "faculties_processed": len(all_stats),
        "total_images_uploaded": sum(s.images_uploaded for s in all_stats),
        "total_duplicates": sum(s.images_duplicate for s in all_stats),
        "total_rejected_invalid": sum(s.images_rejected_invalid for s in all_stats),
        "total_rejected_too_large": sum(s.images_rejected_too_large for s in all_stats),
        "total_skipped_cached": sum(s.images_skipped_cached for s in all_stats),
        "total_ignored_filtered": sum(s.images_ignored_filtered for s in all_stats),
        "total_pages_skipped_blocked": sum(s.pages_skipped_blocked for s in all_stats),
        "total_errors": sum(s.errors for s in all_stats),
        "per_faculty": {
            s.faculty: {
                "pages_visited": s.pages_visited,
                "pages_skipped_blocked": s.pages_skipped_blocked,
                "images_found": s.images_found,
                "images_uploaded": s.images_uploaded,
                "duplicates": s.images_duplicate,
                "rejected_invalid": s.images_rejected_invalid,
                "rejected_too_large": s.images_rejected_too_large,
                "skipped_cached": s.images_skipped_cached,
                "ignored_filtered": s.images_ignored_filtered,
                "errors": s.errors,
            }
            for s in all_stats
        },
    }

    logger.success(
        f"=== Ingestion IMAGES terminée | "
        f"{summary['total_images_uploaded']} images uploadées | "
        f"{summary['total_duplicates']} doublons évités | "
        f"{summary['total_skipped_cached']} déjà en cache (runs précédents) | "
        f"{summary['total_errors']} erreurs totales ==="
    )
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingestion des images des facultés UH2C vers MinIO (raw-images-dev)."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--faculty",
        nargs="+",
        choices=list(UH2C_FACULTY_SEEDS.keys()),
        help="Une ou plusieurs facultés à traiter (ex: --faculty fsac fsbm)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Traiter TOUTES les facultés connues",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY_SECONDS,
        help=(
            f"Délai en secondes entre deux requêtes HTTP (défaut: {REQUEST_DELAY_SECONDS}). "
            "Un jitter aléatoire est automatiquement ajouté par-dessus. "
            "Augmenter cette valeur réduit le risque de captcha/rate-limit."
        ),
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=MAX_PAGES_PER_FACULTY,
        help=f"Nombre maximum de pages crawlées par faculté (défaut: {MAX_PAGES_PER_FACULTY}).",
    )
    return parser


if __name__ == "__main__":
    import json as _json

    args = _build_arg_parser().parse_args()
    selected_faculties = None if args.all else args.faculty

    try:
        result = run(faculties=selected_faculties, delay=args.delay, max_pages=args.max_pages)
        print(_json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as exc:
        logger.critical(f"Échec fatal de l'ingestion d'images: {exc}")