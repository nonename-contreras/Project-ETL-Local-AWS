import os
import sys
from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow/dags/data/src")

from silver_transformation_coingecko import transformar_coingecko_silver
from silver_transformation_github import transformar_github_silver
from silver_validation_coingecko import COINGECKO_SCHEMA, validar_archivo_bronze
from silver_validation_github import GITHUB_SCHEMA, validar_archivo_github_bronze

logger = logging.getLogger(__name__)
BUCKET = "none-nixos-s3-etl"

def get_spark():
    return (
        SparkSession.builder.appName("Procesamiento_Silver")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.shuffle.partitions", "2") 
        .getOrCreate()
    )

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id="02_validacion_transformacion_silver",
    start_date=datetime(2026, 10, 1),
    schedule_interval=None,  
    catchup=False,
    default_args=default_args,
    tags=["silver", "pyspark", "transformacion"]
)
def pipeline_silver():

    @task
    def procesar_silver_crypto(**context):
        conf = context["dag_run"].conf or {}
        ruta_archivo = conf.get("ruta_crypto")
        s3_path = f"s3a://{BUCKET}/{ruta_archivo}"
        spark = get_spark()
        
        try:
            if not ruta_archivo:
                raise ValueError("No se recibio 'ruta_crypto' en dag_run.conf.")

            logger.info(f"Validando y transformando desde: {s3_path}")
            df_valid, df_rejected = validar_archivo_bronze(spark, s3_path, COINGECKO_SCHEMA)

            if df_rejected.count() > 0:
                df_rejected.write.mode("append").json(f"s3a://{BUCKET}/rejected/crypto/")

            df_silver = transformar_coingecko_silver(df_valid)
            
            fecha_ejecucion = context["ds"]
            
            ruta_salida = f"s3a://{BUCKET}/silver/crypto/fecha={fecha_ejecucion}/"
            
            df_silver.write.mode("overwrite").parquet(ruta_salida)
        finally:
            spark.stop()

    @task
    def procesar_silver_github(**context):
        conf = context["dag_run"].conf or {}
        ruta_archivo = conf.get("ruta_github")
        
        s3_path = f"s3a://{BUCKET}/{ruta_archivo}"
        spark = get_spark()

        try:
            logger.info(f"Validando y transformando desde: {s3_path}")
            df_valid, df_rejected = validar_archivo_github_bronze(spark, s3_path, GITHUB_SCHEMA)

            if df_rejected.count() > 0:
                df_rejected.write.mode("append").json(f"s3a://{BUCKET}/rejected/github/")

            df_silver = transformar_github_silver(df_valid)
            
            fecha_ejecucion = context["ds"]
            
            ruta_salida = f"s3a://{BUCKET}/silver/github/fecha={fecha_ejecucion}/"
            
            df_silver.write.mode("overwrite").parquet(ruta_salida)
        finally:
            spark.stop()

    disparar_carga_rds = TriggerDagRunOperator(
        task_id="trigger_carga_rds",
        trigger_dag_id="03_procesamiento_silver_a_rds",
        wait_for_completion=False
    )

    [procesar_silver_crypto(), procesar_silver_github()] >> disparar_carga_rds

pipeline_silver()