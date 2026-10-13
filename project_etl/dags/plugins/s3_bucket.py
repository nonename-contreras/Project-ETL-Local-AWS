
import json
import logging
from typing import Union

import boto3
from botocore.exceptions import ClientError
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

AWS_CONNECTION_ID = "aws_data"
DEFAULT_REGION = "us-east-1"


def json_loaded(datos: Union[dict, list], nombre_bucket: str, ruta_s3: str) -> None:
    destino = f"s3://{nombre_bucket}/{ruta_s3}"

    logger.info("Iniciando carga a S3: %s", destino)

    try:
        conexion = BaseHook.get_connection(AWS_CONNECTION_ID)

        access_key = conexion.login
        secret_key = conexion.password
        region = conexion.extra_dejson.get(
            "region_name",
            DEFAULT_REGION,
        )

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

        contenido_json = json.dumps(
            datos,
            ensure_ascii=False,
        )

        s3_client.put_object(
            Bucket=nombre_bucket,
            Key=ruta_s3,
            Body=contenido_json.encode("utf-8"),
            ContentType="application/json",
        )

        logger.info("Carga exitosa en S3: %s", destino)

    except ClientError:
        logger.exception("Error de AWS al cargar el archivo: %s", destino)
        raise

    except Exception:
        logger.exception("Error inesperado durante la carga a S3: %s", destino)
        raise
