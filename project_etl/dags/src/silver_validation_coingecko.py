import logging

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger("SilverValidation")

COINGECKO_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("last_updated", StringType(), True),
    ]
)

def validar_archivo_bronze(spark, s3_path, schema):
    logger.info("Iniciando validación del archivo: %s", s3_path)

    try:
        df_raw = (
            spark.read
            .option("multiline", "true")
            .schema(schema)
            .json(s3_path)
        )
    except Exception:
        logger.exception("Fallo crítico al leer el archivo desde S3: %s", s3_path)
        raise

    cols_lower = [col.lower() for col in df_raw.columns]

    if len(cols_lower) != len(set(cols_lower)):
        raise ValueError(f"Estructura corrupta: se detectaron columnas duplicadas en {s3_path}.")

    df_eval = df_raw.withColumn(
        "rejection_reason",
        F.when(
            F.col("id").isNull() | (F.trim(F.col("id")) == ""),
            F.lit("ID_NULO_O_VACIO"),
        )
        .when(F.col("current_price").isNull(), F.lit("PRECIO_NULO"))
        .otherwise(F.lit(None)),
    )

    ventana = Window.partitionBy("id").orderBy(
        F.col("last_updated").desc_nulls_last()
    )

    df_eval = (
        df_eval
        .withColumn("row_num", F.row_number().over(ventana))
        .withColumn(
            "rejection_reason",
            F.when(F.col("rejection_reason").isNotNull(), F.col("rejection_reason"))
            .when(F.col("row_num") > 1, F.lit("FILA_DUPLICADA"))
            .otherwise(F.lit(None)),
        )
        .drop("row_num")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    df_valid = df_eval.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    df_rejected = df_eval.filter(F.col("rejection_reason").isNotNull())

    conteo_validos = df_valid.count()
    conteo_rechazados = df_rejected.count()
    
    if (conteo_validos + conteo_rechazados) == 0:
        error_msg = f"El archivo en {s3_path} no contiene filas (archivo vacío)."
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(
        "Validación completada. Filas válidas: %s | Filas rechazadas: %s",
        conteo_validos,
        conteo_rechazados,
    )

    return df_valid, df_rejected