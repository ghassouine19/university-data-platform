import sys
from pathlib import Path

# Bootstrap path projet (pour import jobs.*)
for _parent in Path(__file__).resolve().parents:
    if (_parent / "jobs").is_dir():
        sys.path.insert(0, str(_parent))
        break
else:
    raise RuntimeError("❌ Impossible de localiser le dossier 'jobs/' depuis ce script.")

from jobs.common.spark_session import get_spark_session
from jobs.common.config import settings


def main():
    spark = get_spark_session("Read_Hudi_Publications_Full")
    spark.sparkContext.setLogLevel("WARN")  # optionnel: moins de logs

    path = f"s3a://{settings.MINIO_CURATED_BUCKET}/hudi/silver/research_publications"
    df = spark.read.format("hudi").load(path)

    print(f"\n✅ Rows = {df.count()}\n")

    print("✅ Colonnes:")
    print(df.columns)

    print("\n✅ Schéma complet:")
    df.printSchema()

    print("\n✅ Aperçu complet (toutes colonnes, 50 lignes):")
    df.show(50, truncate=False, vertical=True)


if __name__ == "__main__":
    main()