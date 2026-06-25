import os
import datetime
import hashlib
import json
import boto3
import requests
import urllib3
from botocore.client import Config
from botocore.exceptions import ClientError
from loguru import logger
from typing import List, Optional

# Désactiver les avertissements SSL pour les requêtes locales/API
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_RAW = os.environ.get("MINIO_RAW_BUCKET_JSON", "raw-json-dev")
SOURCE_SYSTEM = "openalex_api"
RESULTS_PER_UNI = 5
UNIVERSITIES_FILE = os.path.join(os.path.dirname(__file__), "universities.txt")

# Client S3 (MinIO)
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
    verify=False,
)

def load_universities(filepath: str) -> List[str]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            universities = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(universities)} universities from {filepath}")
        return universities
    except FileNotFoundError:
        logger.error(f"Universities file not found: {filepath}")
        raise

def ensure_bucket(bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"Bucket already exists: {bucket}")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket created: {bucket}")

def fetch_openalex(university: str) -> Optional[dict]:
    url = f"https://api.openalex.org/works?search={university}&per_page={RESULTS_PER_UNI}"
    try:
        response = requests.get(url, timeout=30, verify=False)
        response.raise_for_status()
        logger.info(f"[{university}] API call successful — HTTP {response.status_code}")
        return response.json()
    except Exception as exc:
        logger.error(f"[{university}] Error: {exc}")
        return None

def build_envelope(raw_data: dict, university: str) -> dict:
    serialized = json.dumps(raw_data, ensure_ascii=False)
    checksum = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    return {
        "metadata": {
            "source_system": SOURCE_SYSTEM,
            "university": university,
            "source_url": f"https://api.openalex.org/works?search={university}",
            "ingestion_timestamp": now_utc.isoformat(),
            "content_hash": checksum,
            "results_count": len(raw_data.get("results", [])),
        },
        "raw_data": raw_data,
    }

def upload_to_minio(payload: dict, university: str) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    key = f"api_ingestion/{university}/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/data_{now.strftime('%H%M%S')}.json"
    s3.put_object(
        Bucket=BUCKET_RAW,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )
    logger.success(f"[{university}] Uploaded → s3://{BUCKET_RAW}/{key}")

def run() -> None:
    universities = load_universities(UNIVERSITIES_FILE)
    logger.info("=" * 60)
    logger.info(f"Ingestion start — source: {SOURCE_SYSTEM}")
    logger.info(f"Target bucket : {BUCKET_RAW}")
    logger.info("=" * 60)

    ensure_bucket(BUCKET_RAW)

    success_count = 0
    failure_count = 0

    for university in universities:
        raw_data = fetch_openalex(university)
        if raw_data is None:
            failure_count += 1
            continue
        envelope = build_envelope(raw_data, university)
        upload_to_minio(envelope, university)
        success_count += 1

    logger.info(f"Ingestion complete — success: {success_count} | failed: {failure_count}")

if __name__ == "__main__":
    run()