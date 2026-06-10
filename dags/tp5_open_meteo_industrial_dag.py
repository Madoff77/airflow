"""
DAG TP5 - Pipeline Airflow Open-Meteo industrialisé.

Ce fichier ne contient que l'orchestration. Toute la logique métier est
déléguée aux modules du package ``tp5`` (extraction, archivage,
transformation, qualité, chargement, traçabilité).

Enchaînement :

    prepare_city_config
      -> fetch_open_meteo_data
      -> archive_raw_data
      -> transform_weather_data
      -> create_tables
      -> run_quality_checks
      -> decide_branch  (branchement conditionnel)
           |-- load_valid_data -> trace_success ------\
           |-- trace_quality_anomaly -----------------+--> end
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from tp5 import config
from tp5.archive import archive_raw_data
from tp5.extract import fetch_open_meteo_data, prepare_city_config
from tp5.load import create_tables, load_valid_data
from tp5.quality import decide_branch, run_quality_checks
from tp5.tracking import trace_quality_anomaly, trace_success
from tp5.transform import transform_weather_data


# Robustesse : 2 tentatives, 1 min entre les essais, garde-fou de durée.
default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    dag_id="tp5_open_meteo_industrial",
    description="Pipeline Open-Meteo industrialisé : archivage, qualité, branchement, traçabilité.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["tp", "tp5", "weather", "open-meteo", "postgres"],
) as dag:

    prepare_city_config_task = PythonOperator(
        task_id="prepare_city_config",
        python_callable=prepare_city_config,
    )

    fetch_open_meteo_data_task = PythonOperator(
        task_id="fetch_open_meteo_data",
        python_callable=fetch_open_meteo_data,
        # Timeout réseau plus serré sur l'appel API externe.
        execution_timeout=timedelta(minutes=3),
    )

    archive_raw_data_task = PythonOperator(
        task_id="archive_raw_data",
        python_callable=archive_raw_data,
    )

    transform_weather_data_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    run_quality_checks_task = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    decide_branch_task = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_branch,
    )

    load_valid_data_task = PythonOperator(
        task_id=config.TASK_LOAD_VALID_DATA,
        python_callable=load_valid_data,
    )

    trace_success_task = PythonOperator(
        task_id="trace_success",
        python_callable=trace_success,
    )

    trace_quality_anomaly_task = PythonOperator(
        task_id=config.TASK_TRACE_QUALITY_ANOMALY,
        python_callable=trace_quality_anomaly,
    )

    # La fin réussit dès qu'une branche aboutit, sans propager le "skipped".
    end_task = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # --- Dépendances -------------------------------------------------------
    prepare_city_config_task >> fetch_open_meteo_data_task >> archive_raw_data_task
    archive_raw_data_task >> transform_weather_data_task >> create_tables_task
    create_tables_task >> run_quality_checks_task >> decide_branch_task

    # Branche conforme : chargement puis traçabilité du succès.
    decide_branch_task >> load_valid_data_task >> trace_success_task >> end_task

    # Branche anomalie : traçabilité de l'anomalie, sans chargement.
    decide_branch_task >> trace_quality_anomaly_task >> end_task
