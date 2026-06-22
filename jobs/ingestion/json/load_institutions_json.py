# jobs/ingestion/files/load_institutions_json.py
"""
Job d'ingestion : telecharge le data dump JSON de ROR (Research
Organization Registry, https://ror.org), filtre les organisations
marocaines de type "education", et envoie le resultat (raw) vers MinIO.

Pourquoi ROR :
    - C'est un vrai FICHIER statique publie periodiquement (pas une API
      live paginee) -> respecte la distinction "fichier" vs "API" du brief.
    - Format JSON officiel, schema documente (ROR Schema v2.1).
    - Licence CC0 (domaine public).

Particularite technique :
    Le fichier publie sur Zenodo est une archive .zip contenant le JSON
    (et un CSV, ignore ici). On telecharge le zip en memoire, on
    l'extrait en memoire avec zipfile/io, on ne touche JAMAIS le disque.

    URL source (dump complet, ~120 000 organisations dans le monde) :
    https://zenodo.org/records/17953395/files/v2.0-2025-12-16-ror-data.zip?download=1

NE MODIFIE AUCUN MODULE EXISTANT (config.py, exceptions.py, logger.py,
minio_client.py). Ce script consomme uniquement ce qui est deja expose :
    - settings              (jobs.common.config)
    - logger                (jobs.common.logger)
    - MinioStorageClient    (jobs.common.minio_client)
    - les exceptions custom (jobs.common.exceptions)

Convention de stockage (bucket MINIO_RAW_BUCKET_JSON) :
    files/year=YYYY/month=MM/day=DD/<basename>__<checksum8>.json
    files/year=YYYY/month=MM/day=DD/<basename>__<checksum8>.metadata.json

Idempotence :
    Le nom d'objet inclut les 8 premiers caracteres du checksum SHA-256
    du JSON FILTRE (Maroc + education). Avant upload, on verifie via
    head_object si l'objet existe deja -> pas de doublon en cas de rerun.
"""

import io
import os
import sys
import json
import zipfile
import hashlib
from datetime import datetime, timezone

import httpx
from botocore.exceptions import ClientError

from jobs.common.config import settings
from jobs.common.logger import logger
from jobs.common.minio_client import MinioStorageClient
from jobs.common.exceptions import (
    SourceAPIConnectionError,
    SourceAPIHTTPError,
    DataQualityValidationError,
    DataLakeConnectionError,
)

SOURCE_SYSTEM = "file_json_ror_dump"
CONNECTOR_VERSION = "1.0.0"
REQUEST_TIMEOUT_SECONDS = 120.0  # le zip fait ~31 Mo, on laisse de la marge

DEFAULT_SOURCE_URL = (
    "https://zenodo.org/records/17953395/files/"
    "v2.0-2025-12-16-ror-data.zip?download=1"
)
DEFAULT_COUNTRY_CODE = "MA"  # Maroc (ISO 3166-1 alpha-2)
DEFAULT_ORG_TYPE = "education"


# ---------------------------------------------------------------------------
# Etape 1 : Telechargement du ZIP depuis l'URL source (en memoire)
# ---------------------------------------------------------------------------

def fetch_zip_from_url(url: str) -> tuple[bytes, int]:
    """
    Telecharge le contenu brut (le .zip) depuis l'URL avec httpx.
    Ne touche jamais le disque. Leve une exception custom du projet
    en cas d'echec.
    """
    try:
        response = httpx.get(url, timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=True)
    except httpx.RequestError as e:
        raise SourceAPIConnectionError(
            "Impossible de contacter la source ROR (Zenodo).",
            details=str(e),
        ) from e

    if response.status_code >= 400:
        raise SourceAPIHTTPError(
            f"La source ROR a renvoye une erreur HTTP {response.status_code}.",
            details=url,
        )

    return response.content, response.status_code


# ---------------------------------------------------------------------------
# Etape 2 : Extraction du JSON depuis le ZIP (en memoire)
# ---------------------------------------------------------------------------

def extract_json_from_zip(zip_bytes: bytes) -> bytes:
    """
    Ouvre l'archive ZIP en memoire (io.BytesIO) et retourne les bytes
    du fichier .json qu'elle contient. N'ecrit rien sur le disque.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            json_members = [name for name in archive.namelist() if name.endswith(".json")]
            if not json_members:
                raise DataQualityValidationError(
                    "Aucun fichier .json trouve dans l'archive ROR telechargee."
                )
            # Le dump ROR ne contient qu'un seul JSON par version -> on prend le premier
            with archive.open(json_members[0]) as json_file:
                return json_file.read()
    except zipfile.BadZipFile as e:
        raise DataQualityValidationError(
            "Le contenu telecharge n'est pas une archive ZIP valide.",
            details=str(e),
        ) from e


# ---------------------------------------------------------------------------
# Etape 3 : Parsing + filtrage (Maroc + type education)
# ---------------------------------------------------------------------------

def parse_records(raw_bytes: bytes) -> list[dict]:
    """Parse les bytes JSON en liste d'enregistrements ROR (schema v2)."""
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise DataQualityValidationError(
            "Le JSON extrait du zip ROR n'est pas valide.",
            details=str(e),
        ) from e

    if not isinstance(data, list):
        raise DataQualityValidationError(
            "Le dump ROR doit etre une liste d'organisations."
        )

    return data


def _record_country_codes(record: dict) -> set[str]:
    """Extrait les codes pays (ISO alpha-2) presents dans les locations ROR."""
    codes = set()
    for location in record.get("locations", []) or []:
        details = location.get("geonames_details", {}) or {}
        code = details.get("country_code")
        if code:
            codes.add(code.upper())
    return codes


def _record_primary_name(record: dict) -> str | None:
    """Extrait le nom principal (ror_display) d'un enregistrement ROR v2."""
    for name_entry in record.get("names", []) or []:
        if "ror_display" in (name_entry.get("types") or []):
            return name_entry.get("value")
    # repli : premier nom disponible si aucun 'ror_display' marque
    names = record.get("names", []) or []
    return names[0]["value"] if names else None


def filter_records(
    records: list[dict],
    country_code: str,
    org_type: str,
) -> tuple[list[dict], int]:
    """
    Filtre les enregistrements ROR sur le pays et le type d'organisation.
    Retourne les enregistrements valides + le nombre rejete (hors filtre).
    """
    valid_records = []
    rejected_count = 0

    for record in records:
        if not isinstance(record, dict):
            rejected_count += 1
            continue

        types = [t.lower() for t in (record.get("types") or [])]
        countries = _record_country_codes(record)

        if org_type.lower() not in types or country_code.upper() not in countries:
            rejected_count += 1
            continue

        if not record.get("id") or not _record_primary_name(record):
            # contrat minimal non respecte (derive de schema potentielle)
            rejected_count += 1
            continue

        valid_records.append(record)

    return valid_records, rejected_count


def build_simplified_records(records: list[dict]) -> list[dict]:
    """
    Construit une version simplifiee (mais toujours brute/raw) des
    enregistrements pour faciliter la suite du pipeline (Spark), tout
    en gardant le payload ROR original sous 'raw_record' pour tracabilite.
    """
    simplified = []
    for record in records:
        countries = _record_country_codes(record)
        simplified.append(
            {
                "ror_id": record.get("id"),
                "name": _record_primary_name(record),
                "types": record.get("types"),
                "country_codes": sorted(countries),
                "established": record.get("established"),
                "links": record.get("links"),
                "raw_record": record,
            }
        )
    return simplified


# ---------------------------------------------------------------------------
# Etape 4 : Metadonnees techniques
# ---------------------------------------------------------------------------

def compute_checksum(raw_bytes: bytes) -> str:
    """Checksum SHA-256 du contenu (calcule sur le JSON filtre final)."""
    return hashlib.sha256(raw_bytes).hexdigest()


def build_metadata(
    source_url: str,
    http_status: int,
    checksum: str,
    record_count: int,
    rejected_count: int,
    country_code: str,
    org_type: str,
) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "source_system": SOURCE_SYSTEM,
        "source_url": source_url,
        "extraction_timestamp": now.isoformat(),
        "http_status": http_status,
        "content_checksum": checksum,
        "connector_version": CONNECTOR_VERSION,
        "record_count": record_count,
        "rejected_count": rejected_count,
        "filter_country_code": country_code,
        "filter_org_type": org_type,
        "app_env": settings.APP_ENV,
    }


# ---------------------------------------------------------------------------
# Etape 5 : Construction de la cle d'objet MinIO
# ---------------------------------------------------------------------------

def build_object_key(basename: str, checksum: str, now: datetime) -> str:
    checksum_short = checksum[:8]
    return (
        f"files/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
        f"{basename}__{checksum_short}.json"
    )


# ---------------------------------------------------------------------------
# Helpers MinIO (sans toucher a MinioStorageClient)
# ---------------------------------------------------------------------------

def object_exists(client: MinioStorageClient, bucket: str, key: str) -> bool:
    """Verifie l'existence d'un objet via head_object (idempotence)."""
    try:
        client.s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise DataLakeConnectionError(
            "Erreur inattendue lors de la verification de l'objet MinIO.",
            details=str(e),
        ) from e


def put_bytes(client: MinioStorageClient, bucket: str, key: str, data: bytes, content_type: str) -> None:
    """Upload de bytes bruts vers MinIO via le s3_client deja initialise."""
    try:
        client.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
    except ClientError as e:
        raise DataLakeConnectionError(
            f"Echec de l'upload de l'objet '{key}' vers le bucket '{bucket}'.",
            details=str(e),
        ) from e


# ---------------------------------------------------------------------------
# Point d'entree principal
# ---------------------------------------------------------------------------

def run_ingestion(
    source_url: str = DEFAULT_SOURCE_URL,
    basename: str = "ror_institutions_morocco",
    country_code: str = DEFAULT_COUNTRY_CODE,
    org_type: str = DEFAULT_ORG_TYPE,
) -> dict:
    """
    Telecharge le dump ROR (zip), extrait le JSON, filtre Maroc +
    type education, et ingere le resultat vers MinIO (bucket raw JSON).
    Rien n'est jamais ecrit sur le disque local de maniere persistante :
    tout transite en memoire.
    """
    logger.info(f"Demarrage ingestion ROR depuis : {source_url}")

    bucket = settings.MINIO_RAW_BUCKET_JSON
    client = MinioStorageClient()

    if not client.check_connection():
        logger.critical("MinIO inaccessible, arret du job.")
        return {"status": "error", "reason": "minio_unreachable"}

    now = datetime.now(timezone.utc)

    # --- Etape 1 : telechargement du zip ---
    try:
        zip_bytes, http_status = fetch_zip_from_url(source_url)
    except (SourceAPIConnectionError, SourceAPIHTTPError) as e:
        logger.error(f"Echec telechargement : {e}")
        return {"status": "error", "reason": str(e)}

    # --- Etape 2 : extraction du JSON depuis le zip ---
    try:
        raw_json_bytes = extract_json_from_zip(zip_bytes)
    except DataQualityValidationError as e:
        logger.error(f"Echec extraction du zip : {e}")
        return {"status": "error", "reason": str(e)}

    # --- Etape 3 : parsing + filtrage Maroc / education ---
    try:
        all_records = parse_records(raw_json_bytes)
    except DataQualityValidationError as e:
        logger.error(f"Echec parsing du JSON ROR : {e}")
        return {"status": "error", "reason": str(e)}

    logger.info(f"Dump ROR complet parse : {len(all_records)} organisations (monde entier).")

    valid_records, rejected_count = filter_records(all_records, country_code, org_type)
    logger.info(
        f"Filtrage applique (country={country_code}, type={org_type}) : "
        f"{len(valid_records)} organisations retenues, {rejected_count} ecartees."
    )

    if not valid_records:
        logger.warning("Aucun enregistrement valide apres filtrage. Arret du job.")
        return {"status": "skipped", "reason": "no_valid_records"}

    simplified_records = build_simplified_records(valid_records)
    final_payload = json.dumps(simplified_records, indent=2, ensure_ascii=False).encode("utf-8")

    # --- Etape 4 : checksum + metadata (sur le JSON filtre final) ---
    checksum = compute_checksum(final_payload)
    object_key = build_object_key(basename, checksum, now)
    metadata_key = object_key.replace(".json", ".metadata.json")
    metadata = build_metadata(
        source_url, http_status, checksum, len(valid_records), rejected_count, country_code, org_type
    )

    # --- Idempotence : skip si deja present ---
    try:
        if object_exists(client, bucket, object_key):
            logger.info(f"Objet deja present (checksum identique) : {object_key} -> skip upload.")
            return {"status": "skipped_duplicate", "object_key": object_key, "checksum": checksum}
    except DataLakeConnectionError as e:
        logger.error(f"Erreur lors de la verification d'idempotence : {e}")
        return {"status": "error", "reason": str(e)}

    # --- Upload raw + metadata (depuis la memoire, pas le disque) ---
    try:
        put_bytes(client, bucket, object_key, final_payload, content_type="application/json")
        put_bytes(
            client,
            bucket,
            metadata_key,
            json.dumps(metadata, indent=2, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
        )
    except DataLakeConnectionError as e:
        logger.error(f"Echec upload vers MinIO : {e}")
        return {"status": "error", "reason": str(e)}

    logger.success(
        f"Upload reussi -> bucket={bucket} object={object_key} "
        f"({len(valid_records)} institutions marocaines)."
    )

    return {
        "status": "success",
        "bucket": bucket,
        "object_key": object_key,
        "metadata_key": metadata_key,
        "checksum": checksum,
        "record_count": len(valid_records),
        "rejected_count": rejected_count,
    }


# ---------------------------------------------------------------------------
# Execution directe (debug manuel)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SOURCE_URL
    result = run_ingestion(source_url=url)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if result["status"] == "error":
        sys.exit(1)