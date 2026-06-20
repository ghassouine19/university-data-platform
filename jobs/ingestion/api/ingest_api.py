import os
import datetime
import hashlib
import json
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# 1. Chargement de la configuration
if os.path.exists(".env.dev"):
    load_dotenv(".env.dev")
else:
    load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_RAW_NAME = os.getenv("MINIO_RAW_BUCKET_JSON", "raw-json-dev")

# Initialisation du client S3
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def assurer_existence_bucket(nom_bucket):
    """Vérifie si le bucket existe, sinon le crée automatiquement."""
    try:
        s3_client.head_bucket(Bucket=nom_bucket)
    except ClientError:
        print(f"Création automatique du bucket : {nom_bucket}")
        s3_client.create_bucket(Bucket=nom_bucket)

def get_universities(file_path):
    """Lit la liste des universités."""
    if not os.path.exists(file_path):
        return ["fsac", "ucam", "usmba", "uit", "fstm"]
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def collecter_donnees_api(url):
    """Appel API."""
    try:
        response = requests.get(url, timeout=30)
        return response.status_code, response.json()
    except Exception as e:
        print(f" Erreur API : {e}")
        return 500, None

def preparer_structure_json(donnees_brutes, url, code_http):
    """Enrichissement avec des métadonnées."""
    chaine_donnees = json.dumps(donnees_brutes, ensure_ascii=False)
    checksum = hashlib.sha256(chaine_donnees.encode('utf-8')).hexdigest()
    return {
        "metadata": {
            "source_system": "openalex_api",
            "ingestion_timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "content_hash": checksum
        },
        "raw_data": donnees_brutes
    }

def envoyer_vers_minio(payload, nom_bucket, nom_universite):
    """Envoi du fichier vers MinIO."""
    date_actuelle = datetime.datetime.now(datetime.UTC)
    path = f"api_ingestion/{nom_universite}/year={date_actuelle.strftime('%Y')}/month={date_actuelle.strftime('%m')}/day={date_actuelle.strftime('%d')}/data_{date_actuelle.strftime('%H%M%S')}.json"
    
    s3_client.put_object(
        Bucket=nom_bucket,
        Key=path,
        Body=json.dumps(payload, ensure_ascii=False, indent=4),
        ContentType='application/json'
    )
    print(f" Données sauvegardées : {path}")

if __name__ == "__main__":
    # --- Création automatique de tous les buckets nécessaires au démarrage ---
    buckets_a_creer = ["raw-json-dev", "university-raw", "raw-web-html-dev"]
    for b in buckets_a_creer:
        assurer_existence_bucket(b)
    
    # --- Processus d'ingestion ---
    liste_universites = get_universities("universities.txt")
    print(f" Ingestion vers : {BUCKET_RAW_NAME}")
    
    for uni in liste_universites:
        url = f"https://api.openalex.org/works?search={uni}&per_page=5"
        status, data = collecter_donnees_api(url)
        if status == 200:
            package = preparer_structure_json(data, url, status)
            envoyer_vers_minio(package, BUCKET_RAW_NAME, uni)