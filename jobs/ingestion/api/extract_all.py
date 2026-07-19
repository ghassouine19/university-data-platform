import json
import hashlib
from datetime import datetime

from jobs.common.logger import logger
from jobs.common.config import settings
from jobs.common.minio_client import MinioStorageClient
from jobs.ingestion.api.extract_openalex import fetch_openalex_publications


def compute_checksum(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def run_openalex_ingestion(institution_id: str):
    """
    Extraction des publications OpenAlex et stockage
    dans la Bronze Zone (Raw JSON).
    """

    logger.info("=" * 70)
    logger.info("🚀 OPENALEX INGESTION START")
    logger.info("=" * 70)

    target_bucket = settings.MINIO_RAW_BUCKET_JSON

    now = datetime.utcnow()

    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    total = 0

    for publication in fetch_openalex_publications(institution_id):

        try:

            json_bytes = json.dumps(
                publication,
                ensure_ascii=False,
                indent=4
            ).encode("utf-8")

            checksum = compute_checksum(json_bytes)

            record_id = publication["id"].split("/")[-1]

            object_path = (
                f"openalex/"
                f"{year}/{month}/{day}/"
                f"{record_id}.json"
            )

            metadata_path = object_path + ".metadata.json"

            metadata = {

                "record_id": record_id,

                "entity_type": "research_publication",

                "source_system": "openalex",

                "source_category": "api",

                "source_url": publication["id"],

                "schema_version": "1.0",

                "status": "raw",

                "crawl_timestamp": timestamp,

                "ingestion_timestamp": timestamp,

                "payload_checksum": checksum,

                "content_length_bytes": len(json_bytes),

                "language": publication.get("language"),

                "normalized_text": None,

                "business_timestamp": publication.get(
                    "publication_date"
                ),

                "is_deleted": False

            }

            # JSON brut
            MinioStorageClient.s3_client.put_object(
                Bucket=target_bucket,
                Key=object_path,
                Body=json_bytes,
                ContentType="application/json"
            )

            # Metadata
            MinioStorageClient.s3_client.put_object(
                Bucket=target_bucket,
                Key=metadata_path,
                Body=json.dumps(
                    metadata,
                    ensure_ascii=False,
                    indent=4
                ).encode("utf-8"),
                ContentType="application/json"
            )

            total += 1

            logger.success(f"Publication {record_id} enregistrée.")

        except Exception as e:

            logger.error(f"Erreur OpenAlex : {e}")

    logger.info("=" * 70)
    logger.success(f"{total} publications OpenAlex enregistrées.")
    logger.info("=" * 70)

def run_crossref_ingestion(query: str):

    logger.info("=" * 70)
    logger.info("🚀 CROSSREF INGESTION START")
    logger.info("=" * 70)

    target_bucket = settings.MINIO_RAW_BUCKET_JSON

    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    total = 0

    for publication in fetch_crossref_publications(query):

        try:

            json_bytes = json.dumps(
                publication,
                ensure_ascii=False,
                indent=4
            ).encode("utf-8")

            checksum = compute_checksum(json_bytes)

            doi = publication.get("DOI", "unknown")
            record_id = hashlib.sha256(
                doi.encode("utf-8")
            ).hexdigest()

            object_path = (
                f"crossref/"
                f"{year}/{month}/{day}/"
                f"{record_id}.json"
            )

            metadata_path = object_path + ".metadata.json"

            metadata = {

                "record_id": record_id,

                "entity_type": "research_publication",

                "source_system": "crossref",

                "source_category": "api",

                "source_url": f"https://doi.org/{doi}" if doi != "unknown" else None,

                "schema_version": "1.0",

                "status": "raw",

                "crawl_timestamp": timestamp,

                "ingestion_timestamp": timestamp,

                "payload_checksum": checksum,

                "content_length_bytes": len(json_bytes),

                "language": None,

                "normalized_text": None,

                "business_timestamp":
                    publication.get("created", {})
                               .get("date-time"),

                "is_deleted": False

            }

            MinioStorageClient.s3_client.put_object(
                Bucket=target_bucket,
                Key=object_path,
                Body=json_bytes,
                ContentType="application/json"
            )

            MinioStorageClient.s3_client.put_object(
                Bucket=target_bucket,
                Key=metadata_path,
                Body=json.dumps(
                    metadata,
                    ensure_ascii=False,
                    indent=4
                ).encode("utf-8"),
                ContentType="application/json"
            )

            total += 1

            logger.success(f"Crossref {doi} enregistré.")

        except Exception as e:

            logger.error(f"Erreur Crossref : {e}")

    logger.success(f"{total} publications Crossref enregistrées.")