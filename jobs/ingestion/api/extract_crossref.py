# jobs/ingestion/apis/extract_crossref.py

import requests

BASE_URL = "https://api.crossref.org/works"

HEADERS = {
    "User-Agent": "UniversityDataPlatformBot/1.0"
}

ROWS_PER_PAGE = 200


def fetch_crossref_publications():
    """
    Récupère toutes les publications d'une université via Crossref.

    Parameters
    ----------
    institution_name : str
        Exemple : "Université Hassan II de Casablanca"
        Attention : recherche texte libre sur le champ affiliation,
        pas un identifiant structuré comme dans OpenAlex.

    Returns
    -------
    generator
        Retourne chaque publication Crossref.
    """

    cursor = "*"

    while True:

        params = {
            "rows": ROWS_PER_PAGE,
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

        message = payload.get("message", {})
        results = message.get("items", [])

        if not results:
            break

        for publication in results:
            yield publication

        next_cursor = message.get("next-cursor")

        if not next_cursor:
            break

        cursor = next_cursor