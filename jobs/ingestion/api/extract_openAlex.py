# jobs/ingestion/apis/extract_openalex.py

import requests

BASE_URL = "https://api.openalex.org/works"

HEADERS = {
    "User-Agent": "UniversityDataPlatformBot/1.0"
}

PER_PAGE = 200


def fetch_openalex_publications(institution_id: str):
    """
    Récupère toutes les publications d'une université OpenAlex.

    Parameters
    ----------
    institution_id : str
      Exemple : I99297268

    Returns
    -------
    generator
        Retourne chaque publication OpenAlex.
    """

    cursor = "*"

    while True:

        params = {
            "filter": f"institutions.id:{institution_id}",
            "per-page": PER_PAGE,
            "cursor": cursor
        }

        response = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=60
        )

        response.raise_for_status()

        payload = response.json()

        results = payload.get("results", [])

        if not results:
            break

        for publication in results:
            yield publication

        next_cursor = payload["meta"].get("next_cursor")

        if not next_cursor:
            break

        cursor = next_cursor