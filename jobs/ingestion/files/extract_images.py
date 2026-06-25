"""
jobs/ingestion/web/extract_images.py
────────────────────────────────────────────────────────────────────────────
Crawl HTML des facultés UH2C, extraction et upload des IMAGES uniquement
vers MinIO (bucket raw-images-dev).

Le client MinIO (upload_bytes / object_exists) est défini directement
dans ce fichier — pas d'import de jobs.common.minio_client.

Usage:
    python -m jobs.ingestion.web.extract_images                # toutes les facultés
    python -m jobs.ingestion.web.extract_images fsac           # une seule faculté
"""

import re
import sys
import json
import time
import socket
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urljoin, urlparse

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import requests
from bs4 import BeautifulSoup

from jobs.common.config import settings
from jobs.common.logger import logger

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

MAX_DEPTH = 2
MAX_PAGES_PER_FACULTY = 50
REQUEST_DELAY_SECONDS = 1.5
IMAGE_DELAY_SECONDS = 0.3
REQUEST_TIMEOUT_SECONDS = 15
MAX_IMAGE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp"}

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
    "ensam":        "http://www.ensam-casa.ma/",
}


# ─────────────────────────────────────────────────────────────────────────────
# CLIENT MINIO — défini ici, pas d'import externe
# ─────────────────────────────────────────────────────────────────────────────

class MinioStorageClient:
    """Passerelle de transport unifiée pour interagir avec le Data Lake MinIO."""

    def __init__(self) -> None:
        try:
            s3_config = Config(signature_version="s3v4", s3={"addressing_style": "path"})
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=settings.MINIO_ENDPOINT,
                aws_access_key_id=settings.MINIO_ROOT_USER,
                aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
                config=s3_config,
                region_name="us-east-1",
            )
            logger.debug("Client MinIO S3 initialisé avec succès.")
        except Exception as e:
            logger.critical(f"Impossible d'initialiser le client MinIO S3 : {e}")
            raise e

    def check_connection(self) -> bool:
        try:
            self.s3_client.list_buckets()
            return True
        except ClientError as e:
            logger.error(f"Échec de la connexion à MinIO : {e}")
            return False

    def upload_bytes(self, data: bytes, bucket: str, object_name: str, content_type: str) -> bool:
        """Upload des bytes bruts (image) directement en mémoire vers MinIO."""
        try:
            self.s3_client.put_object(Bucket=bucket, Key=object_name, Body=data, ContentType=content_type)
            logger.success(f"[MinIO] ✓ {bucket}/{object_name} ({len(data) / 1024:.1f} KB)")
            return True
        except ClientError as e:
            logger.error(f"[MinIO] Échec upload {bucket}/{object_name}: {e}")
            return False

    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Vérifie si l'objet existe déjà dans MinIO (idempotence)."""
        try:
            self.s3_client.head_object(Bucket=bucket, Key=object_name)
            return True
        except ClientError:
            return False

    def get_json(self, bucket: str, object_name: str) -> Optional[dict]:
        """Lit un objet JSON depuis MinIO. Retourne None s'il n'existe pas."""
        try:
            resp = self.s3_client.get_object(Bucket=bucket, Key=object_name)
            return json.loads(resp["Body"].read())
        except ClientError:
            return None

    def put_json(self, data: dict, bucket: str, object_name: str) -> bool:
        """Écrit un objet JSON dans MinIO."""
        try:
            body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
            self.s3_client.put_object(Bucket=bucket, Key=object_name, Body=body, ContentType="application/json")
            return True
        except ClientError as e:
            logger.error(f"[MinIO] Échec écriture manifest {bucket}/{object_name}: {e}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS — URLs et réseau
# ─────────────────────────────────────────────────────────────────────────────

def is_dns_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(k in msg for k in [
        "name or service not known", "failed to resolve",
        "getaddrinfo failed", "[errno -2]", "[errno 11001]", "[errno 11004]",
    ])


def check_reachable(seed_url: str) -> bool:
    host = urlparse(seed_url).hostname
    try:
        socket.setdefaulttimeout(5)
        socket.getaddrinfo(host, None)
        return True
    except OSError as e:
        logger.warning(f"[DNS] {host} inaccessible: {e}")
        return False


def get_base_domain(url: str) -> str:
    return urlparse(url).netloc


def is_same_domain(url: str, base_domain: str) -> bool:
    netloc = urlparse(url).netloc
    return netloc == base_domain or netloc.endswith("." + base_domain)


def normalize_url(raw_href: str, base_url: str) -> Optional[str]:
    if not raw_href:
        return None
    raw_href = raw_href.strip()
    if raw_href.startswith(("#", "mailto:", "javascript:", "tel:", "data:")):
        return None
    absolute = urljoin(base_url, raw_href)
    if not absolute.startswith(("http://", "https://")):
        return None
    return absolute.split("#")[0]


def is_image_url(url: str) -> bool:
    path = urlparse(url).path.lower().split("?")[0]
    return any(path.endswith(ext) for ext in IMAGE_EXTENSIONS)


def url_to_safe_filename(url: str) -> str:
    filename = urlparse(url).path.split("/")[-1].split("?")[0]
    filename = re.sub(r"[^a-zA-Z0-9\-_.]", "-", filename)
    return filename or "image"


def fetch_url(url: str, stream: bool = False) -> Optional[requests.Response]:
    """Fetch avec retry simple (2x). Retourne None si échec définitif. DNS jamais retried."""
    attempt = 0
    while attempt <= 2:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS, stream=stream)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 10)))
                attempt += 1
                continue
            if 400 <= resp.status_code < 500:
                logger.warning(f"[HTTP {resp.status_code}] {url} — skip")
                return None
            if resp.status_code >= 500:
                if attempt < 2:
                    time.sleep(5)
                    attempt += 1
                    continue
                return None
            return resp
        except requests.exceptions.ConnectionError as e:
            if is_dns_error(e):
                logger.error(f"[DNS] {url} inaccessible: {e}")
                return None
            if attempt < 2:
                time.sleep(3)
                attempt += 1
            else:
                return None
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            if attempt < 2:
                time.sleep(3)
                attempt += 1
            else:
                logger.error(f"[RequestException] {url}: {e}")
                return None
    return None


def extract_links_and_images(html_content: str, page_url: str) -> tuple[list[str], list[str]]:
    """Parse une page HTML : retourne (liens internes, URLs d'images) dédupliqués."""
    soup = BeautifulSoup(html_content, "lxml")
    base_domain = get_base_domain(page_url)
    page_links, image_links, seen = [], [], set()

    def add(url: Optional[str], bucket_list: list) -> None:
        if url and url not in seen:
            seen.add(url)
            bucket_list.append(url)

    for tag in soup.find_all("a", href=True):
        url = normalize_url(tag["href"], page_url)
        if url and is_same_domain(url, base_domain) and not is_image_url(url):
            add(url.rstrip("/") + "/", page_links)

    for tag in soup.find_all("img"):
        if tag.get("src"):
            add(normalize_url(tag["src"], page_url), image_links)
        for part in tag.get("srcset", "").split(","):
            add(normalize_url(part.strip().split(" ")[0], page_url), image_links)

    for tag in soup.find_all(style=True):
        for raw in re.findall(r'url\(["\']?([^"\'()]+)["\']?\)', tag["style"]):
            url = normalize_url(raw, page_url)
            if url and is_image_url(url):
                add(url, image_links)

    return page_links, image_links


# ─────────────────────────────────────────────────────────────────────────────
# CORE — Crawl + ingestion images (BFS) pour une faculté
# ─────────────────────────────────────────────────────────────────────────────

def download_and_upload_image(
    minio: MinioStorageClient, image_url: str, faculty_slug: str, run_date: str,
    visited: set, hash_manifest: Dict[str, str],
) -> str:
    """
    Télécharge une image, vérifie si son contenu (hash MD5) a déjà été
    uploadé n'importe quel jour précédent pour cette faculté.
    Retourne: "saved" | "duplicate" | "skipped" | "error"

    - "duplicate" = contenu déjà présent dans MinIO (autre jour ou même jour) → PAS re-uploadé
    - "saved"     = image réellement nouvelle → uploadée
    """
    if image_url in visited:
        return "skipped"
    visited.add(image_url)

    time.sleep(IMAGE_DELAY_SECONDS)
    resp = fetch_url(image_url, stream=True)
    if not resp:
        logger.error(f"[IMG] ✗ Échec téléchargement: {image_url}")
        return "error"

    if int(resp.headers.get("Content-Length", 0)) > MAX_IMAGE_SIZE_BYTES:
        logger.warning(f"[IMG] Trop volumineuse, skip: {image_url}")
        return "skipped"

    img_bytes = b""
    try:
        for chunk in resp.iter_content(chunk_size=16384):
            img_bytes += chunk
            if len(img_bytes) > MAX_IMAGE_SIZE_BYTES:
                return "skipped"
    except requests.exceptions.RequestException as e:
        logger.error(f"[IMG] ✗ Connexion perdue pendant le téléchargement: {image_url} — {e}")
        return "error"

    if not img_bytes:
        return "error"

    # ── Vérification doublon par contenu (hash), valable tous jours confondus ──
    content_hash = hashlib.md5(img_bytes).hexdigest()
    existing_object = hash_manifest.get(content_hash)
    if existing_object:
        logger.info(f"[IMG] ⚠ Doublon détecté (déjà stocké sous {existing_object}) — PAS uploadé: {image_url}")
        return "duplicate"

    bucket = settings.MINIO_RAW_BUCKET_IMAGES
    object_name = f"{faculty_slug}/{run_date}/{url_to_safe_filename(image_url)}"
    content_type = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()

    ok = minio.upload_bytes(img_bytes, bucket, object_name, content_type)
    if ok:
        hash_manifest[content_hash] = object_name
        logger.info(f"[IMG] ✓ Nouvelle image uploadée: {object_name}")
        return "saved"
    return "error"


def crawl_faculty_images(minio: MinioStorageClient, faculty_slug: str, seed_url: str, run_date: str) -> dict:
    """BFS sur les pages HTML d'une faculté ; upload uniquement les images nouvelles (par contenu)."""
    stats = {
        "pages_visited": 0, "images_saved": 0, "images_duplicate": 0,
        "images_skipped": 0, "images_error": 0, "html_errors": 0,
    }

    if not check_reachable(seed_url):
        logger.error(f"[{faculty_slug}] Domaine inaccessible — abandon: {seed_url}")
        return {**stats, "faculty": faculty_slug, "status": "dns_unreachable"}

    # ── Charge le manifest de hash (doublons tous jours confondus) ────────────
    manifest_bucket = settings.MINIO_RAW_BUCKET_IMAGES
    manifest_key = f"{faculty_slug}/_hashes.json"
    hash_manifest: Dict[str, str] = minio.get_json(manifest_bucket, manifest_key) or {}
    logger.info(f"[{faculty_slug}] Manifest chargé: {len(hash_manifest)} images déjà connues")

    visited_pages, visited_images = set(), set()
    queue: list[tuple[str, int]] = [(seed_url, 0)]

    logger.info(f"[{faculty_slug}] Début crawl images | seed={seed_url}")

    try:
        while queue:
            url, depth = queue.pop(0)
            if url in visited_pages or stats["pages_visited"] >= MAX_PAGES_PER_FACULTY:
                continue
            visited_pages.add(url)

            time.sleep(REQUEST_DELAY_SECONDS)
            resp = fetch_url(url)
            if not resp:
                stats["html_errors"] += 1
                continue

            stats["pages_visited"] += 1
            page_links, image_links = extract_links_and_images(resp.text, url)
            logger.info(f"[{faculty_slug}] depth={depth} | {url} → {len(image_links)} images")

            for img_url in image_links:
                result = download_and_upload_image(minio, img_url, faculty_slug, run_date, visited_images, hash_manifest)
                stats[f"images_{result}"] += 1

            # ── Sauvegarde de sécurité du manifest toutes les 10 pages ────────
            # Évite de perdre des heures de progrès si le crawl plante plus tard.
            if stats["pages_visited"] % 10 == 0:
                minio.put_json(hash_manifest, manifest_bucket, manifest_key)

            if depth < MAX_DEPTH:
                for link in page_links:
                    if link not in visited_pages:
                        queue.append((link, depth + 1))
    finally:
        # ── Sauvegarde finale du manifest, même en cas de crash imprévu ───────
        minio.put_json(hash_manifest, manifest_bucket, manifest_key)
        logger.info(f"[{faculty_slug}] Manifest sauvegardé: {len(hash_manifest)} images au total")

    logger.success(
        f"[{faculty_slug}] Terminé | Pages={stats['pages_visited']} | "
        f"Nouvelles={stats['images_saved']} | Doublons={stats['images_duplicate']} | "
        f"Skip={stats['images_skipped']} | Erreurs={stats['images_error']}"
    )
    return {**stats, "faculty": faculty_slug, "status": "completed"}


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def run(faculty_slug: Optional[str] = None) -> Dict[str, Any]:
    """
    Lance l'extraction d'images.
    - faculty_slug=None   → toutes les facultés de UH2C_FACULTY_SEEDS
    - faculty_slug="fsac" → une seule faculté
    """
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    targets = UH2C_FACULTY_SEEDS if faculty_slug is None else {faculty_slug: UH2C_FACULTY_SEEDS.get(faculty_slug)}

    if faculty_slug and targets.get(faculty_slug) is None:
        raise ValueError(f"Faculté inconnue: '{faculty_slug}'. Choix possibles: {list(UH2C_FACULTY_SEEDS)}")

    minio = MinioStorageClient()
    if not minio.check_connection():
        raise ConnectionError(f"MinIO inaccessible: {settings.MINIO_ENDPOINT}")

    logger.info(f"=== Extraction Images démarrée | facultés={list(targets)} | date={run_date} ===")
    results = [crawl_faculty_images(minio, slug, url, run_date) for slug, url in targets.items()]
    logger.success("=== Extraction Images terminée pour toutes les facultés cibles ===")
    return {"run_date": run_date, "results": results}


if __name__ == "__main__":
    import json
    target = sys.argv[1] if len(sys.argv) > 1 else None
    print(json.dumps(run(target), indent=2))