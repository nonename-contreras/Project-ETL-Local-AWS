
import logging

import requests

logger = logging.getLogger(__name__)

URL_GITHUB = "https://api.github.com/repos"
TIMEOUT = 30

HEADERS = {
    "Accept": "application/vnd.github.v3+json",
}


def api_github(usuario: str, repositorio: str) -> dict:
    url = f"{URL_GITHUB}/{usuario}/{repositorio}"

    logger.info(
        "Iniciando extracción de GitHub para el repositorio: %s/%s",
        usuario,
        repositorio,
    )

    try:
        respuesta = requests.get(
            url,
            headers=HEADERS,
            timeout=TIMEOUT,
        )

        respuesta.raise_for_status()
        datos = respuesta.json()

        logger.info(
            "Extracción exitosa. El repositorio '%s' tiene %s estrellas.",
            repositorio,
            datos.get("stargazers_count"),
        )

        return datos

    except requests.exceptions.Timeout:
        logger.exception(
            "Timeout al consultar el repositorio %s/%s.",
            usuario,
            repositorio,
        )
        raise

    except requests.exceptions.HTTPError:
        logger.exception(
            "Error HTTP al consultar el repositorio %s/%s.",
            usuario,
            repositorio,
        )
        raise

    except requests.exceptions.ConnectionError:
        logger.exception(
            "Error de conexión con GitHub para %s/%s.",
            usuario,
            repositorio,
        )
        raise

    except requests.exceptions.JSONDecodeError:
        logger.exception(
            "GitHub devolvió una respuesta JSON inválida para %s/%s.",
            usuario,
            repositorio,
        )
        raise

    except requests.exceptions.RequestException:
        logger.exception(
            "Error en la solicitud a GitHub para %s/%s.",
            usuario,
            repositorio,
        )
        raise
