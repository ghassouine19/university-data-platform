# jobs/common/metadata.py
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

# ==============================================================================
# 📥 1. SCHÉMAS BRONZE (ENTRÉE) : POUR LES FICHIERS DE MÉTADONNÉES D'AUDIT
# ==============================================================================
# (Déjà défini à l'étape précédente pour les documents scrapés)
BRONZE_DOCUMENT_METADATA_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("entity_type", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_category", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("status", StringType(), True),
    StructField("crawl_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("file_extension", StringType(), True),
    StructField("mime_type", StringType(), True),
    StructField("content_length_bytes", IntegerType(), True),
    StructField("payload_checksum", StringType(), True),
    StructField("language", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("business_timestamp", StringType(), True),
    StructField("is_deleted", BooleanType(), True),
    StructField("extracted_title", StringType(), True)
])

BRONZE_API_METADATA_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("entity_type", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_category", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("status", StringType(), True),
    StructField("crawl_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("payload_checksum", StringType(), True),
    StructField("content_length_bytes", IntegerType(), True),
    StructField("language", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("business_timestamp", StringType(), True),
    StructField("is_deleted", BooleanType(), True)
])

# ==============================================================================
# 📥 2. SCHÉMAS BRONZE (ENTRÉE) : POUR LES JSON BRUTS DES APIS SCIENTIFIQUES

# B. Schéma d'entrée complet pour l'API Crossref (Structure imbriquée de votre analyse)
BRONZE_CROSSREF_INPUT_SCHEMA = StructType([
    StructField("message", StructType([
        StructField("total-results", IntegerType(), True),
        StructField("items", ArrayType(StructType([
            StructField("title", ArrayType(StringType()), True),
            StructField("URL", StringType(), True),
            StructField("type", StringType(), True),
            StructField("language", StringType(), True),
            StructField("is-referenced-by-count", IntegerType(), True),
            StructField("short-container-title", ArrayType(StringType()), True),
            # Imbrication de la date Crossref (date-parts: [[2023, 3, 14]])
            StructField("published", StructType([
                StructField("date-parts", ArrayType(ArrayType(IntegerType())), True)
            ]), True),
            # Imbrication des auteurs et de leur affiliation textuelle libre
            StructField("author", ArrayType(StructType([
                StructField("given", StringType(), True),
                StructField("family", StringType(), True),
                StructField("affiliation", ArrayType(StructType([
                    StructField("name", StringType(), True)
                ]), True), True)
            ]), True), True)
        ]), True), True)
    ]), True)
])

# A. Schéma d'entrée complet pour l'API OpenAlex (Structure imbriquée de votre analyse)

BRONZE_OPENALEX_INPUT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("doi", StringType(), True),
    StructField("title", StringType(), True),
    StructField("publication_year", IntegerType(), True),
    StructField("publication_date", StringType(), True),
    StructField("language", StringType(), True),
    StructField("type", StringType(), True),
    StructField("cited_by_count", IntegerType(), True),
    # Imbrication du journal (primary_location -> source -> display_name)
    StructField("primary_location", StructType([
        StructField("source", StructType([
            StructField("display_name", StringType(), True)
        ]), True)
    ]), True),
    StructField("primary_location", StructType([
        StructField("source", StructType([
            StructField("issn_l", StringType(), True)
        ]), True)
    ]), True),
    # Imbrication des auteurs et de leurs institutions/pays
    StructField("authorships", ArrayType(StructType([
        StructField("author", StructType([
            StructField("display_name", StringType(), True),
            StructField("orcid", StringType(), True)
        ]), True),
        StructField("institutions", ArrayType(StructType([
            StructField("display_name", StringType(), True)
        ]), True),),
        StructField("countries", ArrayType(StringType()), True)
    ]), True), True),
    # Imbrication des sujets (primary_topic -> subfield -> field -> domain)
    StructField("primary_topic", StructType([
        StructField("display_name", StringType(), True),
        StructField("subfield", StructType([StructField("display_name", StringType(), True)]), True),
        StructField("field", StructType([StructField("display_name", StringType(), True)]), True),
        StructField("domain", StructType([StructField("display_name", StringType(), True)]), True)
    ]), True),
    # Imbrication Open Access
    StructField("open_access", StructType([
        StructField("is_oa", BooleanType(), True)
    ]), True)
])
