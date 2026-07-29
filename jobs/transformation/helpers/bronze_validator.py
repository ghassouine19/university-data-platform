from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from jobs.common.logger import logger


def validate_not_null(df: DataFrame, columns: list) -> DataFrame:
    condition = F.lit(True)
    for c in columns:
        condition = condition & F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != "")

    total = df.count()
    valid_df = df.filter(condition).cache()
    valid_count = valid_df.count()
    rejected = total - valid_count

    if rejected > 0:
        logger.warning(f"{rejected} enregistrement(s) rejeté(s) : {columns} nul(s) ou vide(s).")
    logger.info(f"{valid_count} / {total} enregistrements valides (non-null sur {columns}).")

    return valid_df


def validate_uniqueness(df: DataFrame, columns: list) -> DataFrame:
    before = df.count()
    deduped_df = df.dropDuplicates(columns)
    after = deduped_df.count()

    if before != after:
        logger.warning(f"{before - after} doublon(s) supprimé(s) sur {columns}.")
    logger.info(f"{after} enregistrement(s) unique(s) sur {columns}.")

    return deduped_df