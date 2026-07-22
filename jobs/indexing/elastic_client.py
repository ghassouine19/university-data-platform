from elasticsearch import Elasticsearch
from jobs.common.config import settings
from jobs.common.logger import logger


class ElasticsearchClient:
    """
    Client Singleton Elasticsearch.

    Ce client est partagé par tous les scripts
    d'indexation afin d'éviter plusieurs connexions.
    """

    _client = None

    @classmethod
    def get_client(cls) -> Elasticsearch:
        """
        Retourne une connexion Elasticsearch.
        """

        if cls._client is None:

            logger.info("Connexion à Elasticsearch...")

            cls._client = Elasticsearch(
                hosts="http://elasticsearch:9200",
                basic_auth=(
                    settings.ELASTIC_USER,
                    settings.ELASTIC_PASSWORD,
                ),
                verify_certs=False,
                request_timeout=60,
            )

            if not cls._client.ping():
                raise ConnectionError(
                    "Impossible de se connecter à Elasticsearch."
                )

            logger.success("Connexion Elasticsearch établie.")

        return cls._client