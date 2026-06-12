import io
import json
from typing import Any, Dict, List, Optional
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import httpx

from jobs.common.config import settings
from jobs.common.logger import logger


class MinioStorageClient:
    """Passerelle de transport unifiée pour interagir avec le Data Lake MinIO."""

    def __init__(self) -> None:
        """Initialise le client S3 en utilisant les configurations centralisées."""
        try:
            # Configuration technique obligatoire pour MinIO (Path-Style Access)
            s3_config = Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"}
            )

            # Instanciation de la ressource boto3 globale
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=settings.MINIO_ENDPOINT,
                aws_access_key_id=settings.MINIO_ROOT_USER,
                aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
                config=s3_config,
                region_name="us-east-1"  # Région par défaut requise par boto3
            )
            logger.debug("Client MinIO S3 initialisé avec succès.")
        except Exception as e:
            logger.critical(f"Impossible d'initialiser le client MinIO S3 : {e}")
            raise e

    def check_connection(self) -> bool:
        """Vérifie si le Data Lake est en ligne et accessible."""
        try:
            self.s3_client.list_buckets()
            return True
        except ClientError as e:
            logger.error(f"Échec de la connexion à MinIO : {e}")
            return False

#0000000000000000000000000000000000000000000000000000000000000000000000000000000000
    #on doit definit les methodes d'upload ici pour centraliser le processus d'ingestion
    #par exemple
    """upload_json(data, bucket, object_name) : Reçoit un dictionnaire Python (vos données universitaires brutes fraîchement extraites d'une API),
     le convertit automatiquement en texte JSON et l'envoie directement dans la couche Bronze (raw/) de MinIO,
      sans avoir à sauvegarder de fichier temporaire sur votre disque Windows."""