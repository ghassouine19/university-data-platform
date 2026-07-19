# jobs/ingestion/api/extract_orcid.py

import requests
from jobs.common.logger import logger

BASE_URL = "https://pub.orcid.org/v3.0"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "UniversityDataPlatformBot/1.0"
}

def search_orcids(query: str ="affiliation-org-name:Hassan II University", rows: int = 200):
    """
    Retourne la liste des identifiants ORCID.
    """

    url = f"{BASE_URL}/search"

    params = {
        "q": query,
        "rows": rows
    }

    response = requests.get(
        url,
        params=params,
        headers=HEADERS,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    results = data.get("result", [])

    logger.info(f"{len(results)} ORCID trouvés.")

    return [
        item["orcid-identifier"]["path"]
        for item in results
        if "orcid-identifier" in item
    ]

def fetch_orcid_profile(orcid: str):
    """
    Retourne le profil complet d'un chercheur.
    """

    url = f"{BASE_URL}/{orcid}"

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=30
    )

    response.raise_for_status()

    return response.json()

def iter_profiles(query="affiliation-org-name:Hassan II University", rows=200):
    """
    Générateur des profils ORCID.
    """

    orcids = search_orcids(query=query, rows=rows)

    for orcid in orcids:

        try:
            yield fetch_orcid_profile(orcid)

        except Exception as e:
            logger.warning(f"Impossible de récupérer {orcid} : {e}")