from pyspark.sql import DataFrame
from jobs.common.logger import logger


def join_raw_with_metadata(raw_df: DataFrame, meta_df: DataFrame, how: str = "inner") -> DataFrame:
    """
    Fusionne le payload brut et les métadonnées d'audit sur '_file_key'.

    Si un même nom de colonne existe des deux côtés (ex: 'language', présent
    à la fois dans le payload OpenAlex/Crossref ET dans le schéma metadata
    générique), la version metadata est écartée avant la jointure pour
    éviter une AMBIGUOUS_REFERENCE : c'est le contenu du payload brut qui
    fait foi, la metadata sert uniquement à l'audit (record_id,
    crawl_timestamp, payload_checksum...).
    """
    overlapping = (set(raw_df.columns) & set(meta_df.columns)) - {"_file_key"}
    if overlapping:
        logger.info(f"Colonnes en conflit raw/metadata écartées côté metadata : {sorted(overlapping)}")
        meta_df = meta_df.drop(*overlapping)

    logger.info(f"Fusion raw + metadata sur '_file_key' (how='{how}')...")
    joined_df = raw_df.join(meta_df, on="_file_key", how=how)
    return joined_df.drop("_file_key")