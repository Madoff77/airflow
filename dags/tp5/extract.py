"""
Extraction des données météo depuis l'API Open-Meteo.

Ce module gère :

- la préparation de la liste des villes (défaut ou dag_run.conf) ;
- l'appel HTTP réel à l'API avec timeout et gestion d'erreurs claire.
"""

import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tp5 import config
from tp5.logging_utils import get_logger

logger = get_logger("extract")


def _validate_city_config(cities):
    """
    Valide et normalise une liste de villes.

    Chaque ville doit être un dictionnaire avec city, latitude, longitude.
    """
    if not isinstance(cities, list) or not cities:
        raise ValueError("La configuration des villes doit être une liste non vide.")

    validated_cities = []
    required_fields = ["city", "latitude", "longitude"]

    for index, city_config in enumerate(cities, start=1):
        if not isinstance(city_config, dict):
            raise ValueError(f"La ville numéro {index} doit être un dictionnaire.")

        missing_fields = [
            field for field in required_fields if field not in city_config
        ]
        if missing_fields:
            raise ValueError(
                f"Champs manquants pour la ville numéro {index}: "
                f"{', '.join(missing_fields)}"
            )

        validated_cities.append(
            {
                "city": str(city_config["city"]),
                "latitude": float(city_config["latitude"]),
                "longitude": float(city_config["longitude"]),
            }
        )

    return validated_cities


def prepare_city_config(**context):
    """
    Prépare la liste des villes à traiter.

    Utilise dag_run.conf["cities"] si fourni, sinon les villes par défaut.
    """
    dag_run = context.get("dag_run")
    configured_cities = None

    if dag_run and dag_run.conf:
        configured_cities = dag_run.conf.get("cities")

    if configured_cities is not None:
        logger.info("Configuration personnalisée reçue via dag_run.conf.")
        cities = _validate_city_config(configured_cities)
    else:
        logger.info("Aucune configuration personnalisée, utilisation des villes par défaut.")
        cities = _validate_city_config(config.DEFAULT_CITIES)

    for city_config in cities:
        logger.info(
            "Ville préparée: %s (%s, %s)",
            city_config["city"],
            city_config["latitude"],
            city_config["longitude"],
        )

    logger.info("%d villes préparées.", len(cities))
    return cities


def fetch_open_meteo_data(ti):
    """
    Appelle l'API Open-Meteo pour chaque ville et renvoie le JSON brut.

    Lève une exception explicite en cas d'erreur HTTP, réseau ou JSON, ce qui
    permet à Airflow de déclencher les retries configurés.
    """
    city_configs = ti.xcom_pull(task_ids="prepare_city_config")

    if not city_configs:
        raise ValueError("Aucune ville reçue depuis prepare_city_config.")

    raw_weather_data = []

    for city_config in city_configs:
        city_name = city_config["city"]
        params = {
            "latitude": city_config["latitude"],
            "longitude": city_config["longitude"],
            "current": ",".join(config.CURRENT_FIELDS),
            "timezone": "Europe/Paris",
        }
        url = f"{config.OPEN_METEO_URL}?{urlencode(params)}"
        request = Request(url, headers={"User-Agent": config.USER_AGENT})

        logger.info("Appel de l'API Open-Meteo pour %s.", city_name)

        try:
            with urlopen(request, timeout=config.HTTP_TIMEOUT_SECONDS) as response:
                status_code = response.getcode()
                response_body = response.read().decode("utf-8")
        except HTTPError as exc:
            raise RuntimeError(
                f"Erreur HTTP lors de la récupération météo pour {city_name}: "
                f"{exc.code} {exc.reason}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(
                f"Erreur réseau lors de la récupération météo pour {city_name}: "
                f"{exc.reason}"
            ) from exc

        if status_code != 200:
            raise RuntimeError(
                f"Réponse inattendue de l'API Open-Meteo pour {city_name}: "
                f"status HTTP {status_code}"
            )

        try:
            api_response = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Réponse JSON invalide reçue pour {city_name}: {exc}"
            ) from exc

        raw_weather_data.append(
            {
                "city": city_name,
                "latitude": city_config["latitude"],
                "longitude": city_config["longitude"],
                "api_response": api_response,
            }
        )
        logger.info("Données brutes récupérées pour %s.", city_name)

    logger.info("%d réponses brutes récupérées.", len(raw_weather_data))
    return raw_weather_data
