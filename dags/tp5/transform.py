"""
Transformation du JSON brut Open-Meteo en lignes prêtes pour PostgreSQL.

On ne conserve que les champs utiles et on les renomme de façon explicite.

Un drapeau de test ``simulate_quality_anomaly`` (via dag_run.conf) permet
d'injecter volontairement une valeur impossible afin de démontrer le cas
d'anomalie qualité de bout en bout, sans jamais corrompre l'API réelle.
"""

from datetime import datetime, timezone

from tp5.logging_utils import get_logger

logger = get_logger("transform")

# Valeur physiquement impossible utilisée uniquement pour la démonstration
# du cas d'anomalie qualité.
_ANOMALY_TEMPERATURE = 999.0


def transform_weather_data(ti, **context):
    """
    Transforme les réponses brutes en lignes normalisées.

    Vérifie la présence de la clé ``current`` et des champs obligatoires.
    """
    raw_weather_data = ti.xcom_pull(task_ids="fetch_open_meteo_data")

    if not raw_weather_data:
        raise ValueError("Aucune réponse brute Open-Meteo reçue.")

    dag_run = context.get("dag_run")
    simulate_anomaly = bool(
        dag_run and dag_run.conf and dag_run.conf.get("simulate_quality_anomaly")
    )
    if simulate_anomaly:
        logger.warning(
            "Drapeau simulate_quality_anomaly actif : une valeur impossible "
            "sera injectée pour démontrer le contrôle qualité."
        )

    required_current_fields = [
        "time",
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "weather_code",
    ]
    ingestion_date = (
        datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    )
    prepared_weather_data = []

    for raw_city_data in raw_weather_data:
        city_name = raw_city_data["city"]
        api_response = raw_city_data["api_response"]

        if "current" not in api_response:
            raise ValueError(f"Clé 'current' manquante dans la réponse de {city_name}.")

        current_weather = api_response["current"]
        missing_fields = [
            field for field in required_current_fields if field not in current_weather
        ]
        if missing_fields:
            raise ValueError(
                f"Champs obligatoires manquants pour {city_name}: "
                f"{', '.join(missing_fields)}"
            )

        prepared_row = {
            "city": city_name,
            "latitude": raw_city_data["latitude"],
            "longitude": raw_city_data["longitude"],
            "observation_time": current_weather["time"],
            "temperature_celsius": current_weather["temperature_2m"],
            "humidity_percent": current_weather["relative_humidity_2m"],
            "wind_speed_kmh": current_weather["wind_speed_10m"],
            "weather_code": current_weather["weather_code"],
            "ingestion_date": ingestion_date,
        }
        prepared_weather_data.append(prepared_row)
        logger.info("Ligne préparée pour %s.", city_name)

    # Injection contrôlée d'une anomalie sur la première ligne, pour la démo.
    if simulate_anomaly and prepared_weather_data:
        target = prepared_weather_data[0]
        logger.warning(
            "Injection d'une température impossible (%.1f) pour %s.",
            _ANOMALY_TEMPERATURE,
            target["city"],
        )
        target["temperature_celsius"] = _ANOMALY_TEMPERATURE

    logger.info("%d lignes météo transformées.", len(prepared_weather_data))
    return prepared_weather_data
