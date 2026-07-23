from fastapi import APIRouter
from elasticsearch import Elasticsearch

router = APIRouter()

es = Elasticsearch(
    "http://elasticsearch:9200",
    basic_auth=("elastic", "elastic_password"),
    verify_certs=False,
)

@router.get("/search")
def search(q: str, size: int = 10):

    response = es.search(
        index="research_publications",
        query={
            "multi_match": {
                "query": q,
                "fields": [
                    "title",
                    "authors",
                    "journal_name"
                ]
            }
        },
        size=size
    )

    return response["hits"]["hits"]