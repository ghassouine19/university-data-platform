import sys
from pathlib import Path

for _parent in Path(__file__).resolve().parents:
    if (_parent / "jobs").is_dir():
        sys.path.insert(0, str(_parent))
        break
else:
    raise RuntimeError("Impossible de localiser le dossier 'jobs/'.")

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from jobs.common.spark_session import get_spark_session
from jobs.common.config import settings
from jobs.common.logger import logger
from jobs.common.metadata import (
    BRONZE_OPENALEX_INPUT_SCHEMA,
    BRONZE_API_METADATA_SCHEMA,
    BUSINESS_KEYS,
    SILVER_PUBLICATIONS_SCHEMA,
)
from jobs.transformation.readers.bronze_reader import _list_files
from jobs.transformation.helpers.bronze_joiner import join_raw_with_metadata
from jobs.transformation.helpers.bronze_validator import validate_not_null, validate_uniqueness
from jobs.transformation.helpers.cleaner import clean_publications
from jobs.common.hudi_writer import write_hudi


def _file_key_from_path(path_col):
    fname = F.regexp_extract(path_col, r"([^/]+)$", 1)
    return F.regexp_replace(fname, r"\.metadata\.json$|\.json$", "")


def _normalize_openalex(df, obj_col="r"):
    return df.select(
        F.col(f"{obj_col}.id").alias("id"),
        F.col(f"{obj_col}.doi").alias("doi"),
        F.col(f"{obj_col}.title").alias("title"),
        F.col(f"{obj_col}.publication_year").cast("int").alias("publication_year"),
        F.col(f"{obj_col}.publication_date").alias("publication_date"),
        F.col(f"{obj_col}.language").alias("language"),
        F.col(f"{obj_col}.primary_location.source.display_name").alias("journal_name"),
        F.col(f"{obj_col}.type").alias("publication_type"),
        F.expr(f"transform({obj_col}.authorships, x -> x.author.display_name)").alias("authors"),
        F.col(f"{obj_col}.primary_topic.display_name").alias("primary_topic"),
        F.col(f"{obj_col}.primary_topic.subfield.display_name").alias("subfield"),
        F.col(f"{obj_col}.primary_topic.field.display_name").alias("field"),
        F.col(f"{obj_col}.primary_topic.domain.display_name").alias("domain"),
        F.col(f"{obj_col}.open_access.is_oa").cast("boolean").alias("is_open_access"),
        F.col(f"{obj_col}.cited_by_count").cast("int").alias("cited_by_count"),
        F.col("_file_key"),
    )


def transform_publications():
    spark = get_spark_session("Publications_Silver_Final")
    print("=" * 80)
    print("serializer =", spark.sparkContext.getConf().get("spark.serializer"))
    print("registrator =", spark.sparkContext.getConf().get("spark.kryo.registrator"))
    print("=" * 80)

    bucket = settings.MINIO_RAW_BUCKET_JSON
    base_path = f"s3a://{bucket}/openalex/"
    all_paths = _list_files(spark, base_path)

    raw_paths = [p for p in all_paths if ".metadata." not in p]
    meta_paths = [p for p in all_paths if ".metadata." in p]

    logger.info(f"RAW files: {len(raw_paths)} | META files: {len(meta_paths)}")
    if not raw_paths:
        raise RuntimeError("Aucun fichier RAW openalex trouvé.")

    # 1) Lire en texte ligne par ligne
    lines_df = spark.read.text(raw_paths).withColumn("_file_path", F.input_file_name())

    # 2) Recomposer JSON complet par fichier
    raw_text = (
        lines_df
        .withColumn("_file_key", _file_key_from_path(F.col("_file_path")))
        .groupBy("_file_path", "_file_key")
        .agg(F.concat_ws("\n", F.collect_list("value")).alias("json_text"))
    )

    # 3) Parse format direct OpenAlex
    direct_obj = raw_text.select(
        F.from_json(F.col("json_text"), BRONZE_OPENALEX_INPUT_SCHEMA).alias("r"),
        F.col("_file_key"),
    ).where(F.col("r.id").isNotNull())

    direct_df = _normalize_openalex(direct_obj, "r")

    # 4) Parse format wrapper {"results":[...]}
    wrapped_arr = raw_text.select(
        F.from_json(
            F.get_json_object(F.col("json_text"), "$.results"),
            ArrayType(StringType())
        ).alias("arr"),
        F.col("_file_key"),
    ).where(F.col("arr").isNotNull())

    wrapped_items = wrapped_arr.select(
        F.explode("arr").alias("item_json"),
        F.col("_file_key")
    )

    wrapped_obj = wrapped_items.select(
        F.from_json(F.col("item_json"), BRONZE_OPENALEX_INPUT_SCHEMA).alias("r"),
        F.col("_file_key"),
    ).where(F.col("r.id").isNotNull())

    wrapped_df = _normalize_openalex(wrapped_obj, "r")

    raw_df = direct_df.unionByName(wrapped_df, allowMissingColumns=True)

    raw_count = raw_df.count()
    logger.info(f"Raw parsed rows: {raw_count}")
    if raw_count == 0:
        raise RuntimeError("❌ 0 lignes RAW parsées (id null partout).")

    # 5) Lire metadata
    meta_df = (
        spark.read.schema(BRONZE_API_METADATA_SCHEMA).json(meta_paths)
        if meta_paths
        else spark.createDataFrame([], BRONZE_API_METADATA_SCHEMA)
    ).withColumn("_file_key", _file_key_from_path(F.input_file_name()))

    logger.info(f"Metadata rows: {meta_df.count()}")

    # 6) Join raw + metadata
    enriched_df = join_raw_with_metadata(raw_df, meta_df, how="left")

    # 7) Mapping Silver
    silver_df = enriched_df.select(
        F.col("id").alias("publication_id"),
        F.col("doi"),
        F.col("title"),
        F.col("publication_year"),
        F.col("publication_date"),
        F.col("language"),
        F.col("journal_name"),
        F.col("publication_type"),
        F.col("authors"),
        F.lit("Université Hassan II de Casablanca").alias("university_name"),
        F.col("primary_topic"),
        F.col("subfield"),
        F.col("field"),
        F.col("domain"),
        F.col("is_open_access"),
        F.col("cited_by_count"),
        F.col("record_id"),
        F.col("crawl_timestamp"),
        F.col("payload_checksum"),
        F.current_timestamp().alias("last_updated_timestamp"),
    )

    # 8) Clean
    silver_df = clean_publications(silver_df)

    # Normalized text pour indexation/recherche sémantique
    silver_df = silver_df.withColumn(
        "normalized_text",
        F.concat_ws(
            " ",
            F.coalesce(F.col("title"), F.lit("")),
            F.coalesce(F.col("doi"), F.lit("")),
            F.coalesce(F.col("journal_name"), F.lit("")),
            F.coalesce(F.col("publication_type"), F.lit("")),
            F.coalesce(F.col("primary_topic"), F.lit("")),
            F.coalesce(F.col("subfield"), F.lit("")),
            F.coalesce(F.col("field"), F.lit("")),
            F.coalesce(F.col("domain"), F.lit("")),
            F.coalesce(F.col("university_name"), F.lit("")),
            F.coalesce(F.col("language"), F.lit("")),
            F.coalesce(F.concat_ws(" ", F.col("authors")), F.lit(""))
        )
    )

    # Nettoyage texte (espaces multiples + lowercase + trim)
    silver_df = (
        silver_df
        .withColumn("normalized_text", F.regexp_replace(F.col("normalized_text"), r"\s+", " "))
        .withColumn("normalized_text", F.lower(F.trim(F.col("normalized_text"))))
    )

    # 9) Forcer exactement le schéma cible
    target_cols = [f.name for f in SILVER_PUBLICATIONS_SCHEMA.fields]
    silver_df = silver_df.select(*target_cols)

    # 10) Validation (une seule fois, après cleaning)
    silver_df = validate_not_null(silver_df, BUSINESS_KEYS["research_publications"])
    silver_df = validate_uniqueness(silver_df, BUSINESS_KEYS["research_publications"])

    final_count = silver_df.count()
    logger.info(f"Silver final rows: {final_count}")
    if final_count == 0:
        raise RuntimeError("0 lignes finales après validation.")

    # 11) Write Hudi (Hive Sync OFF temporaire)
    write_hudi(
        df=silver_df,
        table_name="research_publications",
        record_key="publication_id",
        precombine_key="last_updated_timestamp",
        partition_fields="publication_year,university_name",
        database="silver",
        mode="append",
        enable_hive_sync=False,
    )

    # 12) Snapshot parquet (même colonnes que SILVER_PUBLICATIONS_SCHEMA)
    target = f"s3a://{settings.MINIO_CURATED_BUCKET}/silver/publications/"
    silver_df.write.mode("overwrite").partitionBy("publication_year", "university_name").parquet(target)

    logger.success("Pipeline Silver Publications terminé avec succès.")


if __name__ == "__main__":
    transform_publications()