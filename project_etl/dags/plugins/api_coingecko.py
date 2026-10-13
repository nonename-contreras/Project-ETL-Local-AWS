
import logging
import requests

logger = logging.getLogger(__name__)

URL_COINGECKO = "https://api.coingecko.com/api/v3/coins/markets"

PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 250,
    "page": 1,
    "sparkline": "false",
}

TIMEOUT = 10


def api_coingecko() -> list:
    logger.info("Iniciando extracción desde CoinGecko: %s", URL_COINGECKO)

    try:
        respuesta = requests.get(
            URL_COINGECKO,
            params=PARAMS,
            timeout=TIMEOUT,
        )

        respuesta.raise_for_status()
        datos = respuesta.json()

        logger.info(
            "Extracción exitosa desde CoinGecko. Registros recibidos: %d",
            len(datos) if isinstance(datos, list) else 0,
        )

        return datos

    except requests.exceptions.Timeout:
        logger.error("Timeout al consultar CoinGecko.")
        raise

    except requests.exceptions.HTTPError:
        logger.exception("Error HTTP al consultar CoinGecko.")
        raise

    except requests.exceptions.ConnectionError:
        logger.exception("Error de conexión con CoinGecko.")
        raise

    except requests.exceptions.JSONDecodeError:
        logger.exception("CoinGecko devolvió una respuesta JSON inválida.")
        raise

    except requests.exceptions.RequestException:
        logger.exception("Error en la solicitud a CoinGecko.")
        raise
