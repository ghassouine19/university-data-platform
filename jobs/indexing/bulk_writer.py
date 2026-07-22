from elasticsearch.helpers import bulk

from jobs.common.logger import logger
from jobs.indexing.elastic_client import ElasticsearchClient


class ElasticsearchBulkWriter:

    @staticmethod
    def write_dataframe(df, index_name: str, id_column: str = "record_id"):
        """
        Indexe un DataFrame Spark dans Elasticsearch via la Bulk API.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame à indexer.

        index_name : str
            Nom de l'index Elasticsearch.

        id_column : str
            Colonne utilisée comme identifiant Elasticsearch.
        """

        es = ElasticsearchClient.get_client()

        total_rows = df.count()

        logger.info(
            f"Début de l'indexation de {total_rows} documents vers '{index_name}'."
        )

        actions = (
            {
                "_index": index_name,
                "_id": row[id_column],
                "_source": row.asDict(recursive=True)
            }
            for row in df.toLocalIterator()
        )

        success, failed = bulk(
            client=es,
            actions=actions,
            raise_on_error=False
        )

        logger.success(
            f"Indexation terminée : {success} documents indexés."
        )

        if failed:
            logger.warning(
                f"{len(failed)} documents n'ont pas pu être indexés."
            )