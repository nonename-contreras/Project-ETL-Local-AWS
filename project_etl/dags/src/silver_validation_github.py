import logging

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger("SilverValidationGitHub")

GITHUB_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("private", BooleanType(), True),
        StructField("html_url", StringType(), True),
        StructField("description", StringType(), True),
        StructField("stargazers_count", IntegerType(), True),
        StructField("forks_count", IntegerType(), True),
        StructField("language", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

def validar_archivo_github_bronze(spark, s3_path, schema=GITHUB_SCHEMA):
    logger.info("Iniciando validación de archivo GitHub: %s", s3_path)

    try:
        df_raw = (
            spark.read
            .option("multiline", "true")
            .schema(schema)
            .json(s3_path)
        )
    except Exception:
        logger.exception("Fallo crítico al leer el archivo GitHub de S3: %s", s3_path)
        raise

    cols_lower = [col.lower() for col in df_raw.columns]

    if len(cols_lower) != len(set(cols_lower)):
        raise ValueError(
            f"Columnas duplicadas detectadas en archivo GitHub: {s3_path}."
        )

    df_eval = df_raw.withColumn(
        "rejection_reason",
        F.when(
            F.col("id").isNull(),
            F.lit("ID_NULO"),
        )
        .when(
            F.col("full_name").isNull()
            | (F.trim(F.col("full_name")) == ""),
            F.lit("FULL_NAME_VACIO"),
        )
        .otherwise(F.lit(None)),
    )

    ventana = Window.partitionBy("id").orderBy(
        F.col("updated_at").desc_nulls_last()
    )

    df_eval = (
        df_eval
        .withColumn("row_num", F.row_number().over(ventana))
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("rejection_reason").isNotNull(),
                F.col("rejection_reason"),
            )
            .when(
                F.col("row_num") > 1,
                F.lit("REPOSITORIO_DUPLICADO"),
            )
            .otherwise(F.lit(None)),
        )
        .drop("row_num")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    df_valid = df_eval.filter(
        F.col("rejection_reason").isNull()
    ).drop("rejection_reason")

    df_rejected = df_eval.filter(
        F.col("rejection_reason").isNotNull()
    )

    conteo_validos = df_valid.count()
    conteo_rechazados = df_rejected.count()

    if (conteo_validos + conteo_rechazados) == 0:
        error_msg = f"El archivo GitHub en {s3_path} no contiene filas (archivo vacío)."
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(
        "Validación GitHub completa. Válidos: %s | Rechazados: %s",
        conteo_validos,
        conteo_rechazados,
    )

    return df_valid, df_rejected