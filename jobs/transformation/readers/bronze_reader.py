from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType
from jobs.common.config import settings
from jobs.common.logger import logger
from jobs.common.metadata import BRONZE_OPENALEX_INPUT_SCHEMA


def _list_files(spark: SparkSession, base_path: str) -> list:
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI(base_path), hadoop_conf
    )
    path = sc._jvm.org.apache.hadoop.fs.Path(base_path)
    file_statuses = fs.listFiles(path, True)
    paths = []
    while file_statuses.hasNext():
        status = file_statuses.next()
        paths.append(str(status.getPath()))
    return paths


def _extract_file_key(path_col):
    fname = F.regexp_extract(path_col, r"([^/]+)$", 1)
    return F.regexp_replace(fname, r"\.metadata\.json$|\.json$", "")


def _normalize_openalex_struct(df, struct_col: str):
    return df.select(
        F.col(f"{struct_col}.id").alias("id"),
        F.col(f"{struct_col}.doi").alias("doi"),
        F.col(f"{struct_col}.title").alias("title"),
        F.col(f"{struct_col}.publication_year").cast("int").alias("publication_year"),
        F.col(f"{struct_col}.publication_date").alias("publication_date"),
        F.col(f"{struct_col}.language").alias("language"),
        F.col(f"{struct_col}.primary_location.source.display_name").alias("journal_name"),
        F.col(f"{struct_col}.type").alias("publication_type"),
        F.expr(f"transform({struct_col}.authorships, x -> x.author.display_name)").alias("authors"),
        F.col(f"{struct_col}.primary_topic.display_name").alias("primary_topic"),
        F.col(f"{struct_col}.primary_topic.subfield.display_name").alias("subfield"),
        F.col(f"{struct_col}.primary_topic.field.display_name").alias("field"),
        F.col(f"{struct_col}.primary_topic.domain.display_name").alias("domain"),
        F.col(f"{struct_col}.open_access.is_oa").cast("boolean").alias("is_open_access"),
        F.col(f"{struct_col}.cited_by_count").cast("int").alias("cited_by_count"),
        F.col("_file_key")
    )


def read_raw_and_metadata(spark: SparkSession, source_prefix: str, raw_schema=None, metadata_schema=None):
    bucket = settings.MINIO_RAW_BUCKET_JSON
    base_path = f"s3a://{bucket}/{source_prefix}/"
    logger.info(f"Listage récursif des fichiers sous {base_path}")

    all_paths = _list_files(spark, base_path)
    logger.info(f"   → {len(all_paths)} fichiers trouvés.")

    raw_paths = [p for p in all_paths if ".metadata." not in p]
    meta_paths = [p for p in all_paths if ".metadata." in p]

    if not raw_paths:
        raise RuntimeError(f"Aucun fichier RAW trouvé sous {base_path}")

    # Lecture robuste en texte (évite COLUMN_ALREADY_EXISTS de l'inférence json)
    raw_text = (
        spark.read.text(raw_paths)
        .withColumn("_file_path", F.input_file_name())
        .withColumn("_file_key", _extract_file_key(F.col("_file_path")))
    )

    # Cas A: objet OpenAlex direct
    direct_obj = raw_text.select(
        F.from_json(F.col("value"), BRONZE_OPENALEX_INPUT_SCHEMA).alias("obj"),
        F.col("_file_key")
    ).where(F.col("obj").isNotNull())

    direct_df = _normalize_openalex_struct(direct_obj, "obj")

    # Cas B: wrapper {"results":[ ...obj OpenAlex... ]}
    wrapper_schema = "struct<results:array<string>>"
    wrapper = raw_text.select(
        F.from_json(F.col("value"), wrapper_schema).alias("w"),
        F.col("_file_key")
    ).where(F.col("w.results").isNotNull())

    wrapper_items = wrapper.select(
        F.explode(F.col("w.results")).alias("result_json"),
        F.col("_file_key")
    )

    wrapper_obj = wrapper_items.select(
        F.from_json(F.col("result_json"), BRONZE_OPENALEX_INPUT_SCHEMA).alias("obj"),
        F.col("_file_key")
    ).where(F.col("obj").isNotNull())

    wrapper_df = _normalize_openalex_struct(wrapper_obj, "obj")

    raw_df = direct_df.unionByName(wrapper_df, allowMissingColumns=True)

    # Métadonnées
    meta_df = (
        spark.read.schema(metadata_schema).json(meta_paths)
        if meta_paths
        else spark.createDataFrame([], metadata_schema)
    ).withColumn("_file_key", _extract_file_key(F.input_file_name()))

    raw_count = raw_df.count()
    meta_count = meta_df.count()
    logger.info(f"Raw {source_prefix} : {raw_count} | Metadata : {meta_count}")

    if raw_count == 0:
        raise RuntimeError(
            f"Aucune donnée RAW lue pour '{source_prefix}'. Vérifie le format JSON source."
        )

    return raw_df, meta_df