import os
from datetime import datetime
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

logger = logging.getLogger(__name__)
BUCKET = "none-nixos-s3-etl"

def get_spark_jdbc():
    return (
        SparkSession.builder.appName("Carga_Silver_a_RDS")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )

def get_jdbc_credentials():
    hook = PostgresHook(postgres_conn_id='rds_postgres')
    conn = hook.get_connection('rds_postgres')
    
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
    properties = {
        "user": conn.login,
        "password": conn.password,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",       
        "stringtype": "unspecified" 
    }
    return jdbc_url, properties

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id="03_procesamiento_silver_a_rds",
    start_date=datetime(2026, 10, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["silver", "rds", "staging", "pyspark"]
)
def pipeline_silver_to_rds():

    @task
    def cargar_crypto_a_rds(**context): 
        spark = get_spark_jdbc()
        jdbc_url, props = get_jdbc_credentials()
        
        fecha_ejecucion = context["ds"]
        s3_path = f"s3a://{BUCKET}/silver/crypto/fecha={fecha_ejecucion}/"
        
        try:
            logger.info(f"Leyendo datos Silver de: {s3_path}")
            df_crypto = spark.read.parquet(s3_path)
            
            logger.info("Escribiendo en RDS Staging (stg_crypto)...")           
            df_crypto.write.mode("overwrite") \
                .jdbc(url=jdbc_url, table="staging.stg_crypto", properties=props)
                
            logger.info("Carga de Crypto exitosa.")
        finally:
            spark.stop()

    @task
    def cargar_github_a_rds(**context):
        spark = get_spark_jdbc()
        jdbc_url, props = get_jdbc_credentials()
        
        fecha_ejecucion = context["ds"]
        s3_path = f"s3a://{BUCKET}/silver/github/fecha={fecha_ejecucion}/"
        
        try:
            logger.info(f"Leyendo datos Silver de: {s3_path}")
            df_github = spark.read.parquet(s3_path)
            
            logger.info("Escribiendo en RDS Staging (stg_github)...")
            df_github.write.mode("overwrite") \
                .jdbc(url=jdbc_url, table="staging.stg_github", properties=props)
                
            logger.info("Carga de GitHub exitosa.")
        finally:
            spark.stop()

    disparar_dag_gold = TriggerDagRunOperator(
        task_id='disparar_dag_gold',
        trigger_dag_id='04_dag_gold',  
        wait_for_completion=False
    )

    cargar_crypto_a_rds() >> cargar_github_a_rds() >> disparar_dag_gold

pipeline_silver_to_rds()