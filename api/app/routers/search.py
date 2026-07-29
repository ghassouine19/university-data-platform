from fastapi import APIRouter
from elasticsearch import Elasticsearch

from jobs.common.config import settings

router = APIRouter()

es = Elasticsearch(
    f"http://elasticsearch:{settings.ELASTIC_PORT}",
    basic_auth=(
        settings.ELASTIC_USER,
        settings.ELASTIC_PASSWORD,
    ),
    verify_certs=False,
)

@router.get("/search")
def search(q: str, size: int = 100):
    response = es.search(
        index="research_publications",
        query={
            "multi_match": {
                "query": q,
                "fields": [
                    "title^3",
                    "authors^2",
                    "journal_name"
                ]
            }
        },
        collapse={
            "field": "doi"
        },
        size=size
    )

    return response["hits"]["hits"]