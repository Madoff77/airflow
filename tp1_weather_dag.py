from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_weather_data():
    """
    Simule la récupération de données météo depuis une API.
    Dans un vrai pipeline, cette tâche ferait un appel API.
    """
    cities = ["Paris", "Lyon", "Marseille"]

    weather_data = [
        {"city": "Paris", "temperature": 18, "humidity": 70},
        {"city": "Lyon", "temperature": 21, "humidity": 65},
        {"city": "Marseille", "temperature": 25, "humidity": 55},
    ]

    print("Données météo récupérées avec succès.")
    print(f"Villes traitées : {cities}")
    print(weather_data)

    return weather_data


def validate_weather_data():
    """
    Vérifie que les données météo récupérées sont exploitables.
    Ici, on simule une validation simple.
    """
    required_fields = ["city", "temperature", "humidity"]

    sample_data = [
        {"city": "Paris", "temperature": 18, "humidity": 70},
        {"city": "Lyon", "temperature": 21, "humidity": 65},
        {"city": "Marseille", "temperature": 25, "humidity": 55},
    ]

    for row in sample_data:
        for field in required_fields:
            if field not in row:
                raise ValueError(f"Champ manquant : {field}")

    print("Validation terminée : les données sont complètes.")


def load_weather_data():
    """
    Simule le chargement des données météo en base.
    Dans un vrai pipeline, cette tâche insérerait les données dans une table SQL.
    """
    print("Chargement des données météo en base simulé avec succès.")


default_args = {
    "owner": "elyes",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="tp2_weather_pipeline",
    description="Premier DAG Airflow simple pour simuler un pipeline météo",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tp", "weather", "airflow"],
) as dag:

    fetch_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    validate_weather = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
    )

    load_weather = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data,
    )

    fetch_weather >> validate_weather >> load_weather
    