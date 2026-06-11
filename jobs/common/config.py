import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger

# 1. Détermination automatique de la racine du projet
# Utile pour localiser les fichiers .env peu importe où le script est exécuté
BASE_DIR = Path(__file__).resolve().parents[2]

# 2. Détection dynamique de l'environnement (par défaut 'dev')
APP_ENV = os.getenv("APP_ENV", "dev").lower()
env_file_name = ".env.prod" if APP_ENV == "prod" else ".env.dev"
env_file_path = BASE_DIR / env_file_name

if env_file_path.exists():
    logger.info(f"Centralisation Config : Chargement depuis {env_file_name}")
else:
    logger.warning(f"Fichier {env_file_name} introuvable à la racine {BASE_DIR}. Utilisation des variables système.")


class Settings(BaseSettings):
    """Classe de configuration centralisée avec validation automatique Pydantic."""

    # Configuration interne de Pydantic Settings
    model_config = SettingsConfigDict(
        env_file=env_file_path if env_file_path.exists() else None,
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore les variables d'environnement système inutiles
    )

    # ==========================================
    # CONFIGURATION GLOBALE
    # ==========================================
    PROJECT_NAME: str = "university-data-platform"
    APP_ENV: str = "dev"
    DEBUG: bool = True

    # ==========================================
    # DATA LAKE (MINIO)
    # ==========================================
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_ENDPOINT: str
    MINIO_API_PORT: int = 9000
    MINIO_CONSOLE_PORT: int = 9001

    # Vos 3 buckets de l'architecture en Médaillon
    MINIO_RAW_BUCKET_WEB: str
    MINIO_RAW_BUCKET_JSON: str
    MINIO_RAW_BUCKET_IMAGES: str
    MINIO_RAW_BUCKET_DOCUMENTS: str
    MINIO_RAW_BUCKET_LOGS: str

    # ==========================================
    # DATABASE & CATALOG (POSTGRESQL & HIVE)
    # ==========================================
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_PORT: int = 5432
    HIVE_METASTORE_PORT: int = 9083

    # ==========================================
    # MOTEUR DE RECHERCHE & ORCHESTRATION
    # ==========================================
    ELASTIC_PORT: int = 9200
    AIRFLOW_PORT: int = 8082

    # ==========================================
    # SECURITÉ API
    # ==========================================
    API_PORT: int = 8000
    JWT_SECRET: str
    JWT_ALGORITHM: str = "HS256"


# 3. Instanciation du Singleton de configuration
try:
    settings = Settings()
except Exception as e:
    logger.critical(f"Erreur critique de validation des variables d'environnement : {e}")
    raise e