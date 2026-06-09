import json
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CURRENT_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "weather_code",
]
CITIES = [
    {"city": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Lyon", "latitude": 45.7640, "longitude": 4.8357},
    {"city": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]


def prepare_city_config():
    """
    Prepare the city list used by the ingestion pipeline.
    """
    print("Préparation de la configuration des villes.")
    for city_config in CITIES:
        print(
            f"Ville préparée: {city_config['city']} "
            f"({city_config['latitude']}, {city_config['longitude']})"
        )

    return CITIES


def fetch_open_meteo_data(ti):
    """
    Call Open-Meteo and return raw JSON responses for each city.
    """
    city_configs = ti.xcom_pull(task_ids="prepare_city_config")

    if not city_configs:
        raise ValueError("Aucune configuration de ville reçue.")

    raw_weather_data = []

    for city_config in city_configs:
        city_name = city_config["city"]
        params = {
            "latitude": city_config["latitude"],
            "longitude": city_config["longitude"],
            "current": ",".join(CURRENT_FIELDS),
            "timezone": "Europe/Paris",
        }
        url = f"{OPEN_METEO_URL}?{urlencode(params)}"
        request = Request(url, headers={"User-Agent": "airflow-tp3-open-meteo"})

        print(f"Appel de l'API Open-Meteo pour {city_name}.")

        try:
            with urlopen(request, timeout=30) as response:
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
        print(f"Données brutes récupérées pour {city_name} avec succès.")

    return raw_weather_data


def transform_weather_data(ti):
    """
    Transform raw API responses into a clean future target-table structure.
    """
    raw_weather_data = ti.xcom_pull(task_ids="fetch_open_meteo_data")

    if not raw_weather_data:
        raise ValueError("Aucune réponse brute Open-Meteo reçue.")

    required_current_fields = [
        "time",
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "weather_code",
    ]
    ingestion_date = datetime.now(timezone.utc).isoformat(timespec="seconds")
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
        print(f"Données transformées pour {city_name}: {prepared_row}")

    print("Transformation terminée avec succès.")
    return prepared_weather_data


def preview_prepared_data(ti):
    """
    Print a preview of the prepared data that could be stored later.
    """
    prepared_weather_data = ti.xcom_pull(task_ids="transform_weather_data")

    if not prepared_weather_data:
        raise ValueError("Aucune donnée préparée à afficher.")

    target_columns = [
        "city",
        "latitude",
        "longitude",
        "observation_time",
        "temperature_celsius",
        "humidity_percent",
        "wind_speed_kmh",
        "weather_code",
        "ingestion_date",
    ]

    print("Aperçu des données préparées pour une future table cible.")
    print(f"Colonnes cibles: {target_columns}")

    for row in prepared_weather_data:
        print(json.dumps(row, ensure_ascii=False))

    print("Aperçu terminé.")


default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="tp3_open_meteo_ingestion",
    description="Préparer une ingestion API météo avec Open-Meteo.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tp", "weather", "open-meteo", "api"],
) as dag:
    prepare_city_config = PythonOperator(
        task_id="prepare_city_config",
        python_callable=prepare_city_config,
    )

    fetch_open_meteo_data = PythonOperator(
        task_id="fetch_open_meteo_data",
        python_callable=fetch_open_meteo_data,
    )

    transform_weather_data = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    preview_prepared_data = PythonOperator(
        task_id="preview_prepared_data",
        python_callable=preview_prepared_data,
    )

    prepare_city_config >> fetch_open_meteo_data >> transform_weather_data >> preview_prepared_data
