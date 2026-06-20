from datetime import datetime
import requests
from bs4 import BeautifulSoup
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

from jobs.common.config import settings
from jobs.common.minio_client import minio_client


# 🔥 Désactiver warning SSL (propre pour dev)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# -----------------------------
# UNIVERSITÉS MAROCAINES
# -----------------------------
UNIVERSITIES = [
    {"name": "um5", "url": "https://www.um5.ac.ma/"},
    {"name": "uca", "url": "https://www.uca.ma/"},
    {"name": "usmba", "url": "https://www.usmba.ac.ma/"},
    {"name": "uh2c", "url": "https://www.univh2c.ma/"},
    {"name": "usms", "url": "https://www.usms.ac.ma/"},
    {"name": "uiz", "url": "https://www.uiz.ac.ma/"},
]


# -----------------------------
# SESSION ROBUSTE (RETRY + TIMEOUT)
# -----------------------------
def get_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


# -----------------------------
# SCRAPER ROBUSTE
# -----------------------------
def scrape_university(url: str):

    session = get_session()

    headers = {"User-Agent": settings.SCRAPING_USER_AGENT}

    try:
        response = session.get(
            url,
            headers=headers,
            timeout=30,
            verify=False  # 🔥 FIX SSL ERROR
        )

        response.raise_for_status()

    except Exception as e:
        logger.error(f"HTTP ERROR {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    results = []

    for item in soup.find_all(["article", "li", "div"]):

        title = None
        link = None

        h = item.find(["h1", "h2", "h3"])
        a = item.find("a")

        if h:
            title = h.get_text(strip=True)

        if a and a.get("href"):
            link = a["href"]

        if title:
            results.append({
                "title": title,
                "url": link or "",
                "source": url,
                "scraped_at": datetime.utcnow().isoformat()
            })

    # fallback si vide
    if not results:
        text = soup.get_text(" ", strip=True)

        results.append({
            "title": "FULL_PAGE",
            "content": text[:5000],
            "source": url,
            "scraped_at": datetime.utcnow().isoformat()
        })

    return results


# -----------------------------
# PIPELINE PRINCIPAL
# -----------------------------
def run_pipeline():

    logger.info("🚀 START UNIVERSITY DATA INGESTION PIPELINE")

    for uni in UNIVERSITIES:

        name = uni["name"]
        url = uni["url"]

        logger.info(f"SCRAPING {name} -> {url}")

        data = scrape_university(url)

        if not data:
            logger.warning(f"NO DATA -> {name}")
            continue

        now = datetime.now()

        object_path = (
            f"raw/web/{name}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/data.json"
        )

        try:
            minio_client.upload_json(
                bucket_name=settings.MINIO_RAW_BUCKET_WEB,
                object_name=object_path,
                data=data
            )

            logger.info(f"UPLOAD SUCCESS -> {name}")

        except Exception as e:
            logger.error(f"MINIO ERROR {name}: {e}")


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    run_pipeline()