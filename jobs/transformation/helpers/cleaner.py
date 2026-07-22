from pyspark.sql import DataFrame, functions as F


def clean_publications(df: DataFrame) -> DataFrame:
    """
    Cleaning standard Publications (OpenAlex/Crossref unifié côté Silver).
    """
    return (
        df
        .withColumn("publication_id", F.trim(F.col("publication_id")))
        .withColumn("doi", F.lower(F.trim(F.col("doi"))))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("publication_year", F.col("publication_year").cast("int"))
        .withColumn("publication_date", F.to_date(F.col("publication_date")))
        .withColumn("language", F.lower(F.trim(F.col("language"))))
        .withColumn("journal_name", F.trim(F.col("journal_name")))
        .withColumn("publication_type", F.lower(F.trim(F.col("publication_type"))))
        .withColumn("primary_topic", F.trim(F.col("primary_topic")))
        .withColumn("subfield", F.trim(F.col("subfield")))
        .withColumn("field", F.trim(F.col("field")))
        .withColumn("domain", F.trim(F.col("domain")))
        .withColumn("is_open_access", F.col("is_open_access").cast("boolean"))
        .withColumn("cited_by_count", F.col("cited_by_count").cast("int"))
        .withColumn(
            "authors",
            F.expr("filter(transform(authors, x -> trim(x)), x -> x is not null and x != '')")
        )
        .withColumn("record_id", F.trim(F.col("record_id")))
        .withColumn("crawl_timestamp", F.trim(F.col("crawl_timestamp")))
        .withColumn("payload_checksum", F.trim(F.col("payload_checksum")))
        .withColumn("last_updated_timestamp", F.current_timestamp())
    )