import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_openalex():
    from jobs.ingestion.api.extract_all import run_openalex_ingestion
    run_openalex_ingestion(institution_id="I99297268")


with DAG(
    dag_id="university_publications_pipeline",
    description="Ingestion OpenAlex -> Transformation -> Elasticsearch",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["spark", "openalex", "elasticsearch"],
) as dag:

    extract_openalex = PythonOperator(
        task_id="extract_openalex",
        python_callable=_run_openalex,
    )

    transform_publications = BashOperator(
        task_id="transform_publications",
        bash_command="""
    /usr/bin/docker exec \
    ${PROJECT_NAME}_spark_master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/transformation/pipelines/transform_publications.py
    """
    )

    index_publications = BashOperator(
        task_id="index_publications",
        bash_command="""
        /usr/bin/docker exec \
        ${PROJECT_NAME}_spark_master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/indexing/index_publications.py
        """
    )

    elasticsearch_ready = EmptyOperator(
        task_id="elasticsearch_ready"
    )

    extract_openalex >> transform_publications >> index_publications >> elasticsearch_ready