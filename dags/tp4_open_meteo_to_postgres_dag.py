import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
POSTGRES_CONN_ID = "weather_postgres"
CREATE_TABLES_SQL_PATH = Path("/opt/airflow/sql/001_create_weather_tables.sql")
CURRENT_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "weather_code",
]
DEFAULT_CITIES = [
    {"city": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Lyon", "latitude": 45.7640, "longitude": 4.8357},
    {"city": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]


def _validate_city_config(cities):
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
    Prepare default cities or override them with dag_run.conf["cities"].
    """
    dag_run = context.get("dag_run")
    configured_cities = None

    if dag_run and dag_run.conf:
        configured_cities = dag_run.conf.get("cities")

    if configured_cities is not None:
        print("Configuration personnalisée reçue via dag_run.conf.")
        cities = _validate_city_config(configured_cities)
    else:
        print("Aucune configuration personnalisée reçue, utilisation des villes par défaut.")
        cities = _validate_city_config(DEFAULT_CITIES)

    for city_config in cities:
        print(
            f"Ville préparée: {city_config['city']} "
            f"({city_config['latitude']}, {city_config['longitude']})"
        )

    return cities


def fetch_open_meteo_data(ti):
    """
    Call Open-Meteo and return raw JSON responses for each city.
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
            "current": ",".join(CURRENT_FIELDS),
            "timezone": "Europe/Paris",
        }
        url = f"{OPEN_METEO_URL}?{urlencode(params)}"
        request = Request(url, headers={"User-Agent": "airflow-tp4-open-meteo"})

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
        print(f"Données brutes récupérées pour {city_name}.")

    return raw_weather_data


def transform_weather_data(ti):
    """
    Transform raw Open-Meteo JSON into rows ready for PostgreSQL.
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
    ingestion_date = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(
        timespec="seconds"
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
        print(f"Ligne préparée pour PostgreSQL: {prepared_row}")

    print(f"{len(prepared_weather_data)} lignes météo préparées.")
    return prepared_weather_data


def create_weather_tables():
    """
    Create PostgreSQL tables using the SQL script.
    """
    if not CREATE_TABLES_SQL_PATH.exists():
        raise FileNotFoundError(f"Script SQL introuvable: {CREATE_TABLES_SQL_PATH}")

    sql = CREATE_TABLES_SQL_PATH.read_text(encoding="utf-8")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(sql)

    print("Tables PostgreSQL créées ou déjà existantes.")


def load_weather_data_to_postgres(ti):
    """
    Insert prepared weather rows into PostgreSQL without creating duplicates.
    """
    prepared_weather_data = ti.xcom_pull(task_ids="transform_weather_data")

    if not prepared_weather_data:
        raise ValueError("Aucune donnée météo préparée à charger.")

    insert_sql = """
        INSERT INTO weather_observations (
            city,
            latitude,
            longitude,
            observation_time,
            temperature_celsius,
            humidity_percent,
            wind_speed_kmh,
            weather_code,
            ingestion_date
        )
        VALUES (
            %(city)s,
            %(latitude)s,
            %(longitude)s,
            %(observation_time)s,
            %(temperature_celsius)s,
            %(humidity_percent)s,
            %(wind_speed_kmh)s,
            %(weather_code)s,
            %(ingestion_date)s
        )
        ON CONFLICT (city, observation_time)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            temperature_celsius = EXCLUDED.temperature_celsius,
            humidity_percent = EXCLUDED.humidity_percent,
            wind_speed_kmh = EXCLUDED.wind_speed_kmh,
            weather_code = EXCLUDED.weather_code,
            ingestion_date = EXCLUDED.ingestion_date;
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, prepared_weather_data)
        conn.commit()

    print(f"{len(prepared_weather_data)} lignes météo insérées ou mises à jour.")


def write_ingestion_tracking(ti, **context):
    """
    Write one tracking row for the successful ingestion run.
    """
    cities = ti.xcom_pull(task_ids="prepare_city_config") or []
    prepared_weather_data = ti.xcom_pull(task_ids="transform_weather_data") or []

    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    cities_count = len(cities)
    rows_count = len(prepared_weather_data)
    ingestion_date = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(
        timespec="seconds"
    )
    message = f"Ingestion réussie pour {rows_count} lignes météo."

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO ingestion_tracking (
            dag_id,
            run_id,
            ingestion_date,
            cities_count,
            rows_count,
            status,
            message
        )
        VALUES (
            %(dag_id)s,
            %(run_id)s,
            %(ingestion_date)s,
            %(cities_count)s,
            %(rows_count)s,
            %(status)s,
            %(message)s
        );
        """,
        parameters={
            "dag_id": dag_id,
            "run_id": run_id,
            "ingestion_date": ingestion_date,
            "cities_count": cities_count,
            "rows_count": rows_count,
            "status": "success",
            "message": message,
        },
    )

    print(
        "Suivi d'ingestion écrit: "
        f"dag_id={dag_id}, run_id={run_id}, villes={cities_count}, lignes={rows_count}."
    )


default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="tp4_open_meteo_to_postgres",
    description="Pipeline complet Open-Meteo vers PostgreSQL.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tp", "weather", "open-meteo", "postgres"],
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

    create_weather_tables = PythonOperator(
        task_id="create_weather_tables",
        python_callable=create_weather_tables,
    )

    load_weather_data_to_postgres = PythonOperator(
        task_id="load_weather_data_to_postgres",
        python_callable=load_weather_data_to_postgres,
    )

    write_ingestion_tracking = PythonOperator(
        task_id="write_ingestion_tracking",
        python_callable=write_ingestion_tracking,
    )

    prepare_city_config >> fetch_open_meteo_data >> transform_weather_data
    transform_weather_data >> create_weather_tables >> load_weather_data_to_postgres >> write_ingestion_tracking
