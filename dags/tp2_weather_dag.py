from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_weather_data():
    """
    Simule la récupération de données météo pour plusieurs villes.
    """
    weather_data = [
        {"city": "Paris", "temperature": 18, "humidity": 70},
        {"city": "Lyon", "temperature": 21, "humidity": 65},
        {"city": "Marseille", "temperature": 25, "humidity": 55},
    ]

    print("Données météo récupérées avec succès.")
    print("Données récupérées :")
    for row in weather_data:
        print(row)

    return weather_data


def validate_weather_data(ti):
    """
    Vérifie que chaque donnée météo contient les champs obligatoires.
    """
    weather_data = ti.xcom_pull(task_ids="fetch_weather_data")
    required_fields = ["city", "temperature", "humidity"]

    if not weather_data:
        raise ValueError("Aucune donnée météo reçue depuis la tâche fetch_weather_data.")

    for index, row in enumerate(weather_data, start=1):
        for field in required_fields:
            if field not in row:
                raise ValueError(
                    f"Champ obligatoire manquant dans la ligne {index}: {field}. "
                    f"Donnée reçue: {row}"
                )

    print("Validation réussie : toutes les données météo sont complètes.")
    return weather_data


def load_weather_data(ti):
    """
    Simule le chargement des données météo en base.
    """
    weather_data = ti.xcom_pull(task_ids="validate_weather_data")

    print("Chargement simulé des données météo en base.")
    for row in weather_data:
        print(f"Chargement simulé pour {row['city']} : {row}")

    print("Chargement terminé avec succès.")


default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="tp2_weather_pipeline",
    description="Premier DAG Airflow simple simulant un pipeline météo.",
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
