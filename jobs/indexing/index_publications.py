import sys
from pathlib import Path

for _parent in Path(__file__).resolve().parents:
    if (_parent / "jobs").is_dir():
        sys.path.insert(0, str(_parent))
        break
else:
    raise RuntimeError("❌ Impossible de localiser le dossier 'jobs/'.")


from jobs.common.spark_session import get_spark_session
from jobs.common.config import settings
from jobs.common.logger import logger

from jobs.indexing.mappings import create_indices
from jobs.indexing.bulk_writer import ElasticsearchBulkWriter


def index_publications():
    """
    Indexe la table Hudi research_publications
    dans Elasticsearch.
    """

    logger.info("=" * 70)
    logger.info("DÉBUT DE L'INDEXATION DES PUBLICATIONS")
    logger.info("=" * 70)

    spark = get_spark_session("Index Publications")

    hudi_path = (
        f"s3a://{settings.MINIO_CURATED_BUCKET}/hudi/silver/research_publications"
    )

    logger.info(f"Lecture de la table Hudi : {hudi_path}")

    df = (
        spark.read
        .format("hudi")
        .load(hudi_path)
    )

    if df.rdd.isEmpty():

        logger.warning(
            "Aucune publication trouvée."
        )

        spark.stop()
        return

    logger.info(
        f"{df.count()} publications trouvées."
    )

    # Création des index Elasticsearch
    create_indices()

    # Indexation Bulk
    ElasticsearchBulkWriter.write_dataframe(
        df=df,
        index_name="research_publications",
        id_column="record_id"
    )

    spark.stop()

    logger.success(
        "Indexation des publications terminée."
    )


if __name__ == "__main__":
    index_publications()