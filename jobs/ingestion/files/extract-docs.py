import os
import sys
import json
import hashlib
import re
import unicodedata
import mimetypes
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup

# Fixation dynamique du chemin racine pour l'architecture modulaire
sys.path.insert(0, str(Path(__file__).resolve().parents))

from jobs.common.config import settings
from jobs.common.minio_client import MinioStorageClient
from jobs.common.exceptions import SourceAPIHTTPError
from jobs.common.logger import logger
from jobs.common.playwright_client import PlaywrightBrowserSession

# CONFIGURATION — ADRESSES DES FACULTÉS (HASSAN II CASABLANCA)
TARGETS = {
    "fsac":"https://fsac.univh2c.ma/"
}

# Paramètres techniques globaux
EXTENSIONS = ('.pdf', '.doc', '.docx', '.csv', '.xlsx', '.xls', '.txt')
CLOUD_DOMAINS = ('://google.com', '://google.com', 'mediafire.com', 'dropbox.com')
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


# UTILITIES

def compute_checksum(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sanitize_file_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')
    name = re.sub(r'[^a-zA-Z0-9_.-]', '-', name)
    return re.sub(r'-+', '-', name).strip('-')


# CRAWLER HYBRIDE : PLAYWRIGHT + RECURSIVITÉ BFS

def get_links_recursive(session: PlaywrightBrowserSession, root_url: str) -> list:
    """
    Explore récursivement TOUTES les sous-pages d'une faculté en exécutant
    le JavaScript via Playwright (Algorithme de parcours BFS).
    """
    parsed_root = urlparse(root_url)
    target_domain = parsed_root.netloc
    parent_domain = ".".join(target_domain.split(".")[-2:])  # 'univh2c.ma'

    queue = [root_url]  # File d'attente des pages HTML à visiter
    visited_pages = set()  # Anti-boucle infinie
    extracted_elements = []  # Tableau final des documents localisés
    unique_urls = set()  # Anti-doublons de fichiers

    # Limite pour protéger les ressources de la machine Windows en dev local
    MAX_PAGES_TO_CRAWL = 30
    pages_crawled_count = 0

    logger.info(f"🕸️ Démarrage du Crawler Hybride : {target_domain} (Réseau : {parent_domain})")

    # Ouverture d'un onglet unique réutilisé pour ce domaine
    page = session.context.new_page()
    page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)
    while queue and pages_crawled_count < MAX_PAGES_TO_CRAWL:
        current_page = queue.pop(0)

        if current_page in visited_pages:
            continue

        visited_pages.add(current_page)
        pages_crawled_count += 1

        logger.debug(f"  [{pages_crawled_count}/{MAX_PAGES_TO_CRAWL}] Exécution JS & Rendu sur : {current_page}")

        try:
            # au lieu de "networkidle" qui peut bloquer indéfiniment ---
            page.goto(current_page, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)

            # Récupération de tous les liens de la page directement depuis le DOM vivant exécuté
            hrefs = page.evaluate("""() => {
                return Array.from(document.querySelectorAll('a')).map(a => a.href);
            }""")
            logger.debug(f"    → {len(hrefs)} liens bruts trouvés sur {current_page}")
            for href in hrefs:
                if not href:
                    continue
                href = href.strip()
                if href.startswith("#") or href.startswith("javascript:"):
                    continue

                full_url = urljoin(current_page, href)
                parsed_url = urlparse(full_url)
                url_lower = full_url.lower()

                # --- CAS A : DÉTECTION DES DOCUMENTS OU DRIVES ---
                is_document_file = any(ext in url_lower for ext in EXTENSIONS)
                is_cloud_storage = any(cloud in parsed_url.netloc for cloud in CLOUD_DOMAINS)

                if is_document_file or is_cloud_storage:
                    if full_url not in unique_urls:
                        unique_urls.add(full_url)

                        # Récupération du titre textuel via Playwright s'avère plus simple,
                        # ou on extrait le nom du chemin à défaut
                        title_text = parsed_url.path.split("/")[-1] or "document_file"

                        extracted_elements.append({
                            "type": "document",
                            "url": full_url,
                            "title": title_text
                        })
                    continue  # C'est un document, on ne l'ajoute pas à la file des pages HTML

                # --- CAS B : DÉCOUVERTE DE NOUVELLES PAGES HTML INTERNES ---
                # On autorise le robot à planifier une visite si on reste sur le site de la faculté
                if parsed_url.netloc == target_domain:
                    if full_url not in visited_pages and full_url not in queue:
                        # Sécurité basique anti-images
                        if not any(url_lower.endswith(m) for m in ('.jpg', '.png', '.jpeg', '.mp4', '.zip')):
                            queue.append(full_url)

        except Exception as e:
            logger.warning(f" Erreur sur {current_page} : {type(e).__name__}: {e}")
            continue

    page.close()
    logger.info(
        f"Fin du crawl hybride. {pages_crawled_count} pages lues. {len(extracted_elements)} document(s) trouvé(s).")
    return extracted_elements


def scrape_and_store_all():
    logger.info("=" * 70)
    logger.info(" DÉBUT DE L'EXTRACTION PIPELINE HYBRIDE COMPLÈTE")
    logger.info("=" * 70)
    client = MinioStorageClient()
    if not client.check_connection():
        logger.critical("Liaison avec le Data Lake MinIO rompue.")
        raise SourceAPIHTTPError("MinIO indisponible.")

    target_bucket = settings.MINIO_RAW_BUCKET_DOCUMENTS
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    total_files = 0

    # Utilisation du Context Manager de la couche common pour ouvrir le navigateur UNE SEULE FOIS
    with PlaywrightBrowserSession() as session:
        for university_code, url in TARGETS.items():
            logger.info(f"Initialisation de la campagne sur : [{university_code}] -> {url}")
            elements = get_links_recursive(session, url)

            if not elements:
                logger.warning(f"Aucun document découvert sur l'ensemble de l'arborescence de {university_code}")
                continue

            for item in elements:
                file_url = item["url"]
                raw_name = file_url.split("/")[-1].split("?")[0]
                clean_file_name = raw_name if raw_name else "document_unnamed"
                extension = Path(clean_file_name).suffix.lower()

                try:
                    # Téléchargement unifié
                    response = requests.get(file_url, headers=HEADERS, timeout=30)
                    response.raise_for_status()
                    content = response.content

                    checksum = compute_checksum(content)
                    record_id = hashlib.sha256(f"{university_code}:{file_url}".encode("utf-8")).hexdigest()
                    short_id = record_id[:8]
                    safe_name = sanitize_file_name(clean_file_name)

                    object_path = f"{university_code}/{year}/{month}/{day}/{short_id}_{safe_name}"
                    metadata_path = object_path + ".metadata.json"

                    mime_type = response.headers.get("Content-Type") or mimetypes.guess_type(clean_file_name)[
                        0] or "application/octet-stream"

                    metadata_payload = {
                        "record_id": record_id,
                        "entity_type": "document",
                        "source_system": university_code,
                        "source_category": "website",
                        "source_url": file_url,
                        "schema_version": "1.0",
                        "status": "raw",
                        "crawl_timestamp": timestamp,
                        "ingestion_timestamp": timestamp,
                        "file_name": clean_file_name,
                        "file_extension": extension,
                        "mime_type": mime_type,
                        "content_length_bytes": len(content),
                        "payload_checksum": checksum,
                        "language": None,
                        "normalized_text": None,
                        "business_timestamp": None,
                        "is_deleted": False,
                        "extracted_title": item["title"]
                    }

                    client.s3_client.put_object(
                        Bucket=target_bucket,
                        Key=object_path,
                        Body=content,
                        ContentType=mime_type
                    )
                    client.s3_client.put_object(
                        Bucket=target_bucket,
                        Key=metadata_path,
                        Body=json.dumps(metadata_payload, ensure_ascii=False, indent=4).encode("utf-8"),
                        ContentType="application/json"
                    )
                    total_files += 1
                    logger.success(f" [Bronze] {short_id}_{safe_name} synchronisé.")

                except Exception as e:
                    logger.error(f" Échec d'écriture pour l'URL {file_url} : {e}")

    logger.info("=" * 70)
    logger.success(f"Extraction terminée : {total_files} documents enregistrés dans {target_bucket}.")
    logger.info("=" * 70)


if __name__ == "__main__":
    scrape_and_store_all()
