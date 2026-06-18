# jobs/ingestion/files/extract_all_documents.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from jobs.common.config import settings
from jobs.common.minio_client import MinioStorageClient
from jobs.common.logger import logger

import requests
from bs4 import BeautifulSoup
import hashlib
from datetime import date
from urllib.parse import urljoin
from urllib.parse import urlparse

# ============================================================
# CONFIGURATION — URLS ORGANIZED BY FACULTY/SOURCE NAME
# ============================================================
TARGETS = {
    # 1) University institutional websites
    "um5":      "https://www.um5.ac.ma/",
    "uca":      "https://www.uca.ma/",
    "usmba":    "https://www.usmba.ac.ma/",
    "uh2c":     "https://www.uh2c.ac.ma/",
    "usms":     "https://www.usms.ac.ma/",
    "uiz":      "https://www.uiz.ac.ma/",

    # 2) Student services and governance
    "onousc":   "https://www.onousc.ma/",
    "enssup":   "https://www.enssup.gov.ma/",
    "men":      "https://www.men.gov.ma/",

    # 3) Research and publication repositories
    "imist":    "https://www.imist.ma/",
    "toubkal":  "https://toubkal.imist.ma/",

    # 4) National open data and statistics
    "datagov":  "https://www.data.gov.ma/",
    "hcp":      "https://www.hcp.ma/",

    # 5) International academic enrichment APIs
    "openalex": "https://api.openalex.org/",
    "crossref": "https://api.crossref.org/",
    "orcid":    "https://pub.orcid.org/",

    # 6) Training content for mathematics
    "wikipedia_math":       "https://en.wikipedia.org/wiki/Mathematics",
    "wikipedia_math_edu":   "https://en.wikipedia.org/wiki/Category:Mathematics_education",
    "ocw_mit":              "https://ocw.mit.edu/",
    "khanacademy":          "https://www.khanacademy.org/math",
}

# File extensions to download
EXTENSIONS = ('.pdf', '.doc', '.docx', '.csv', '.json', '.xml', '.xls', '.xlsx', '.txt')

HEADERS = {"User-Agent": "UniversityDataPlatformBot/1.0 (+http://localhost)"}

# ============================================================
# FUNCTIONS
# ============================================================

def compute_checksum(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def get_links(url: str) -> list:
    """Extract all downloadable file links from a page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Échec accès {url} : {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = []

    for tag in soup.find_all("a", href=True):
        href = tag['href']
        if any(href.lower().endswith(ext) for ext in EXTENSIONS):
            full_url = urljoin(url, href)
            links.append(full_url)

    return links


def download_and_upload(file_url: str, bucket: str, folder: str, today: str, client) -> bool:
    """Download a file and upload to MinIO."""
    try:
        resp = requests.get(file_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Échec téléchargement {file_url} : {e}")
        return False

    checksum = compute_checksum(resp.content)
    file_name = file_url.split("/")[-1].split("?")[0]  # remove query params

    object_name = f"{folder}/{today}/{file_name}"

    client.s3_client.put_object(
        Bucket=bucket,
        Key=object_name,
        Body=resp.content,
        ContentType="application/octet-stream"
    )

    logger.success(f"✓ {folder} : {file_name} (sha256:{checksum[:12]}...)")
    return True


def scrape_all():
    logger.info("=" * 60)
    logger.info(f"DÉBUT EXTRACTION — {len(TARGETS)} sources")
    logger.info("=" * 60)

    client = MinioStorageClient()
    bucket = settings.MINIO_RAW_BUCKET_DOCUMENTS
    today = date.today().isoformat()

    total_files = 0
    failed_sources = []

    for folder_name, url in TARGETS.items():
        logger.info(f"\n▶ [{folder_name}] {url}")
        links = get_links(url)

        if not links:
            logger.warning(f"Aucun document trouvé pour {folder_name}")
            failed_sources.append(folder_name)
            continue

        logger.info(f"{len(links)} document(s) trouvé(s)")
        for file_url in links:
            if download_and_upload(file_url, bucket, folder_name, today, client):
                total_files += 1

    logger.info("\n" + "=" * 60)
    logger.success(f"EXTRACTION TERMINÉE : {total_files} fichier(s) uploadé(s)")
    if failed_sources:
        logger.warning(f"Sources sans documents : {', '.join(failed_sources)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    scrape_all()