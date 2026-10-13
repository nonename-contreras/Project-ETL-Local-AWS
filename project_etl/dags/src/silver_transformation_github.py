
import logging

from pyspark.sql import functions as F

logger = logging.getLogger("SilverTransformationGitHub")


def transformar_github_silver(df_valid):
    logger.info(
        "Iniciando transformación Silver para repositorios GitHub."
    )

    try:
        df_silver = (
            df_valid
            .withColumnRenamed("id", "repo_id")
            .withColumnRenamed("name", "repo_name")
            .withColumnRenamed("full_name", "repo_full_name")
            .withColumnRenamed("stargazers_count", "stars")
            .withColumnRenamed("forks_count", "forks")
            .withColumnRenamed("language", "primary_language")
            .withColumn(
                "repo_created_at",
                F.to_timestamp(
                    F.col("created_at"),
                    "yyyy-MM-dd'T'HH:mm:ss'Z'",
                ),
            )
            .withColumn(
                "repo_updated_at",
                F.to_timestamp(
                    F.col("updated_at"),
                    "yyyy-MM-dd'T'HH:mm:ss'Z'",
                ),
            )
            .withColumn("processed_at", F.current_timestamp())
            .drop("created_at", "updated_at")
        )

        logger.info(
            "Transformación GitHub finalizada. Columnas: %s",
            df_silver.columns,
        )

        return df_silver

    except Exception:
        logger.exception(
            "Error en la transformación Silver de GitHub."
        )
        raise
