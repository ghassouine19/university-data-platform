import requests

API_URL = "http://localhost:8000"


def search_publications(query: str):
    """
    Recherche des publications via FastAPI.
    """

    if not query:
        return []

    try:

        response = requests.get(
            f"{API_URL}/search",
            params={"q": query},
            timeout=10
        )

        if response.status_code == 200:
            return response.json()

        return []

    except Exception:
        return []