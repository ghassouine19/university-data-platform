class UniversityPlatformException(Exception):
    """Exception de base pour l'ensemble de la plateforme universitaire.

    Toutes nos erreurs personnalisées héritent de celle-ci.
    """

    def __init__(self, message: str, details: str = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} (Détails: {self.details})"
        return self.message


# ==========================================
# 1. INFRASTRUCTURE & DATA LAKE EXCEPTIONS
# ==========================================
class DataLakeConfigurationError(UniversityPlatformException):
    """Levée lorsque les variables d'accès à MinIO sont incorrectes."""
    pass


class DataLakeConnectionError(UniversityPlatformException):
    """Levée lorsque MinIO est hors-ligne ou inaccessible par le réseau."""
    pass


class DataLakeBucketNotFoundError(UniversityPlatformException):
    """Levée lorsqu'un bucket requis (ex: raw, curated) n'existe pas."""
    pass


# ==========================================
# 2. INGESTION & SOURCING EXCEPTIONS
# ==========================================
class SourceAPIConnectionError(UniversityPlatformException):
    """Levée lorsque l'API externe d'une université ne répond pas."""
    pass


class SourceAPIHTTPError(UniversityPlatformException):
    """Levée lorsque l'API renvoie un code d'erreur HTTP (ex: 403, 500)."""
    pass


class ScrapingExtractionError(UniversityPlatformException):
    """Levée lorsque la structure HTML d'un site à scraper a changé."""
    pass


# ==========================================
# 3. DATA QUALITY & GOVERNANCE EXCEPTIONS
# ==========================================
class DataQualityValidationError(UniversityPlatformException):
    """Levée si une donnée brute viole le contrat de schéma (metadata.py)."""
    pass


# ==========================================
# 4. PROCESSING & SPARK EXCEPTIONS
# ==========================================
class SparkJobExecutionError(UniversityPlatformException):
    """Levée lorsqu'un traitement lourd ou une action PySpark échoue."""
    pass


class CatalogMetastoreError(UniversityPlatformException):
    """Levée en cas d'échec de synchronisation avec Hive Metastore."""
    pass