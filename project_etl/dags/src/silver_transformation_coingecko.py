
import logging

from pyspark.sql import functions as F

logger = logging.getLogger("SilverTransformation")


def transformar_coingecko_silver(df_valid):
    logger.info("Iniciando transformación y normalización de CoinGecko Silver.")

    try:
        df_silver = (
            df_valid
            .withColumnRenamed("id", "crypto_id")
            .withColumnRenamed("symbol", "ticker")
            .withColumnRenamed("name", "crypto_name")
            .withColumn(
                "updated_at",
                F.to_timestamp(
                    F.col("last_updated"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                ),
            )
            .withColumn("processed_at", F.current_timestamp())
            .drop("last_updated")
        )

        logger.info(
            "Transformación de CoinGecko finalizada. Columnas: %s",
            df_silver.columns,
        )

        return df_silver

    except Exception:
        logger.exception(
            "Error en la etapa de transformación Silver de CoinGecko."
        )
        raise
