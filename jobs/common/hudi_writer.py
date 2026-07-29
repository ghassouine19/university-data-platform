from pyspark.sql import DataFrame
from jobs.common.config import settings
from jobs.common.logger import logger


def build_hudi_options(
    table_name: str,
    record_key: str,
    precombine_key: str,
    partition_fields: str,
    database: str = "silver",
    table_type: str = "COPY_ON_WRITE",
    enable_hive_sync: bool = False,   # IMPORTANT: False pour éviter ton erreur HMS S3A
) -> dict:
    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine_key,
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    }

    if enable_hive_sync:
        opts.update({
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.mode": "hms",
            "hoodie.datasource.hive_sync.database": database,
            "hoodie.datasource.hive_sync.table": table_name,
            "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        })
    else:
        opts["hoodie.datasource.hive_sync.enable"] = "false"

    return opts


def write_hudi(
    df: DataFrame,
    table_name: str,
    record_key: str,
    precombine_key: str,
    partition_fields: str,
    database: str = "silver",
    mode: str = "append",
    enable_hive_sync: bool = False,
):
    base_path = f"s3a://{settings.MINIO_CURATED_BUCKET}/hudi/{database}/{table_name}"
    options = build_hudi_options(
        table_name=table_name,
        record_key=record_key,
        precombine_key=precombine_key,
        partition_fields=partition_fields,
        database=database,
        enable_hive_sync=enable_hive_sync,
    )

    logger.info(f"Hudi write: {database}.{table_name} -> {base_path}")
    (
        df.write
        .format("hudi")
        .options(**options)
        .mode(mode)
        .save(base_path)
    )
    logger.success(f"Hudi write terminé: {database}.{table_name}")