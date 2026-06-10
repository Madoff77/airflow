"""
Traçabilité des exécutions du pipeline TP5.

Chaque exécution écrit exactement une ligne dans la table de suivi, que le
résultat soit un succès ou une anomalie qualité. La traçabilité est
idempotente : une relance du même run met à jour la ligne existante grâce à
la contrainte d'unicité ``(dag_id, run_id)``.
"""

from datetime import datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook

from tp5 import config
from tp5.logging_utils import get_logger

logger = get_logger("tracking")


def _now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _write_tracking_row(parameters):
    """Insère ou met à jour une ligne de suivi (upsert idempotent)."""
    sql = f"""
        INSERT INTO {config.TRACKING_TABLE} (
            dag_id,
            run_id,
            ingestion_date,
            cities_count,
            rows_received,
            rows_loaded,
            quality_status,
            anomalies_count,
            message
        )
        VALUES (
            %(dag_id)s,
            %(run_id)s,
            %(ingestion_date)s,
            %(cities_count)s,
            %(rows_received)s,
            %(rows_loaded)s,
            %(quality_status)s,
            %(anomalies_count)s,
            %(message)s
        )
        ON CONFLICT (dag_id, run_id)
        DO UPDATE SET
            ingestion_date = EXCLUDED.ingestion_date,
            cities_count = EXCLUDED.cities_count,
            rows_received = EXCLUDED.rows_received,
            rows_loaded = EXCLUDED.rows_loaded,
            quality_status = EXCLUDED.quality_status,
            anomalies_count = EXCLUDED.anomalies_count,
            message = EXCLUDED.message;
    """
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    hook.run(sql, parameters=parameters)


def trace_success(ti, **context):
    """
    Trace une exécution réussie (branche qualité conforme).
    """
    cities = ti.xcom_pull(task_ids="prepare_city_config") or []
    report = ti.xcom_pull(task_ids="run_quality_checks") or {}
    rows_loaded = ti.xcom_pull(task_ids=config.TASK_LOAD_VALID_DATA) or 0

    parameters = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "ingestion_date": _now_iso(),
        "cities_count": len(cities),
        "rows_received": report.get("rows_received", 0),
        "rows_loaded": rows_loaded,
        "quality_status": "success",
        "anomalies_count": 0,
        "message": f"Ingestion réussie : {rows_loaded} lignes chargées.",
    }
    _write_tracking_row(parameters)

    logger.info(
        "Suivi écrit (succès) : run_id=%s, lignes chargées=%s.",
        parameters["run_id"],
        rows_loaded,
    )


def trace_quality_anomaly(ti, **context):
    """
    Trace une anomalie qualité (branche bloquée) : aucune ligne n'est chargée.
    """
    cities = ti.xcom_pull(task_ids="prepare_city_config") or []
    report = ti.xcom_pull(task_ids="run_quality_checks") or {}
    anomalies = report.get("anomalies", [])

    detail = "; ".join(
        f"{a.get('city')}: {', '.join(a.get('reasons', []))}" for a in anomalies
    )
    message = f"Anomalie qualité : chargement bloqué. Détail: {detail}"

    parameters = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "ingestion_date": _now_iso(),
        "cities_count": len(cities),
        "rows_received": report.get("rows_received", 0),
        "rows_loaded": 0,
        "quality_status": "quality_failed",
        "anomalies_count": len(anomalies),
        "message": message,
    }
    _write_tracking_row(parameters)

    logger.warning(
        "Suivi écrit (anomalie qualité) : run_id=%s, anomalies=%d. Aucune donnée chargée.",
        parameters["run_id"],
        len(anomalies),
    )
