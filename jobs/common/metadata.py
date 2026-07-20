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
        StructField("items", ArrayType(StructType([
            StructField("title", ArrayType(StringType()), True),
            StructField("URL", StringType(), True),
            StructField("type", StringType(), True),
            StructField("language", StringType(), True),
            StructField("is-referenced-by-count", IntegerType(), True),
            StructField("container-title", ArrayType(StringType()), True),
            # Imbrication de la date Crossref (date-parts: [[2023, 3, 14]])
            StructField("published", StructType([
                StructField("date-parts", ArrayType(ArrayType(IntegerType())), True)
            ]), True),
            # Imbrication des auteurs et de leur affiliation textuelle libre
            StructField("author", ArrayType(StructType([
                StructField("given", StringType(), True),
                StructField("family", StringType(), True),
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


# ==============================================================================
# SCHÉMA BRONZE (ENTRÉE) : POUR LES PROFILS CHERCHEURS INDIVIDUELS ORCID
# ==============================================================================

BRONZE_ORCID_INPUT_SCHEMA = StructType([
    # 1. Identifiant Unique ORCID (orcid-identifier -> path)
    StructField("orcid-identifier", StructType([
        StructField("path", StringType(), True),
        StructField("uri", StringType(), True)
    ]), True),

    # 2. Données Personnelles du Chercheur (person -> name, emails, biography, addresses)
    StructField("person", StructType([
        # Nom et Prénom (name -> given-names/family-name -> value)
        StructField("name", StructType([
            StructField("given-names", StructType([StructField("value", StringType(), True)]), True),
            StructField("family-name", StructType([StructField("value", StringType(), True)]), True)
        ]), True),
        # Liste des Emails (emails -> email -> [ {value: ...} ])
        StructField("emails", StructType([
            StructField("email", ArrayType(StructType([
                StructField("value", StringType(), True)
            ]), True), True)
        ]), True),
        # Biographie / Description (biography -> content)
        StructField("biography", StructType([
            StructField("content", StringType(), True)
        ]), True),
        # Liste des Adresses pour le pays (addresses -> address -> [ {country: {value: "MA"}} ])
        StructField("addresses", StructType([
            StructField("address", ArrayType(StructType([
                StructField("country", StructType([
                    StructField("value", StringType(), True)
                ]), True)
            ]), True), True)
        ]), True)
    ]), True),

    # 3. Activités et Affiliations Académiques (activities-summary -> employments -> ...)
    StructField("activities-summary", StructType([
        StructField("employments", StructType([
            StructField("affiliation-group", ArrayType(StructType([
                StructField("summaries", ArrayType(StructType([
                    # Département et Rôle occupé (ex: Professor)
                    StructField("department-name", StringType(), True),
                    StructField("role-title", StringType(), True),
                    # Organisation de rattachement (organization -> name: "Hassan II University")
                    StructField("organization", StructType([
                        StructField("name", StringType(), True)
                    ]), True)
                ]), True), True)
            ]), True), True)
        ]), True)
    ]), True)
])


#les clè métier
BUSINESS_KEYS = {

    "research_publications": ["doi"],

    "faculty_profiles": ["orcid"],

    "documents_registry": ["payload_checksum"]

}