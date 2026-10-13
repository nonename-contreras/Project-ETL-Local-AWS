from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

S3_BUCKET = "none-nixos-s3-etl"

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2), 
}

@dag(
    dag_id="01_extraccion_apis_a_s3",
    start_date=datetime(2026, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["extraccion", "bronze", "github", "coingecko"]
)
def pipeline_extraccion():

    @task
    def task_geko(**context):
        from data.plugins.api_coingecko import api_coingecko
        from data.plugins.s3_bucket import json_loaded
        
        logger.info("Iniciando flujo: CoinGecko -> S3")
        datos = api_coingecko()
        
        fecha_ejecucion = context["data_interval_end"].strftime('%Y%m%d_%H%M%S')
        ruta_archivo = f"bronze/crypto/bitcoin_{fecha_ejecucion}.json"
        
        json_loaded(datos, S3_BUCKET, ruta_archivo)
        return ruta_archivo

    @task
    def task_github(**context):
        from data.plugins.api_github import api_github
        from data.plugins.s3_bucket import json_loaded
        
        logger.info("Iniciando flujo: GitHub -> S3")
        datos = api_github("apache", "airflow")
        
        fecha_ejecucion = context["data_interval_end"].strftime('%Y%m%d_%H%M%S')
        ruta_archivo = f"bronze/github/airflow_stats_{fecha_ejecucion}.json"
        
        json_loaded(datos, S3_BUCKET, ruta_archivo)
        return ruta_archivo

    t_crypto = task_geko()
    t_github = task_github()
    
    disparar_silver = TriggerDagRunOperator(
        task_id="disparar_dag_silver",
        trigger_dag_id="02_validacion_transformacion_silver",  
        wait_for_completion=False,                     
        conf={
            "ruta_crypto": t_crypto,
            "ruta_github": t_github
        }
    )

    [t_crypto, t_github] >> disparar_silver

pipeline_extraccion()