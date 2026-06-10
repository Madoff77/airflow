"""
Chargement idempotent des données météo dans PostgreSQL.

L'idempotence repose sur une contrainte d'unicité ``(city, observation_time)``
et une clause ``ON CONFLICT ... DO UPDATE``. Relancer le pipeline pour la même
observation met à jour la ligne au lieu de créer un doublon.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook

from tp5 import config
from tp5.logging_utils import get_logger

logger = get_logger("load")


def create_tables():
    """
    Crée les tables PostgreSQL du TP5 si elles n'existent pas encore.
    """
    if not config.CREATE_TABLES_SQL_PATH.exists():
        raise FileNotFoundError(
            f"Script SQL introuvable: {config.CREATE_TABLES_SQL_PATH}"
        )

    sql = config.CREATE_TABLES_SQL_PATH.read_text(encoding="utf-8")
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    hook.run(sql)

    logger.info("Tables PostgreSQL du TP5 créées ou déjà existantes.")


def load_valid_data(ti):
    """
    Insère uniquement les lignes valides issues du contrôle qualité.

    Cette tâche n'est exécutée que sur la branche "qualité conforme".
    """
    report = ti.xcom_pull(task_ids="run_quality_checks")

    if not report:
        raise ValueError("Aucun rapport qualité disponible pour le chargement.")

    valid_rows = report.get("valid_rows") or []
    if not valid_rows:
        raise ValueError("Aucune ligne valide à charger.")

    insert_sql = f"""
        INSERT INTO {config.WEATHER_TABLE} (
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

    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, valid_rows)
        conn.commit()

    logger.info(
        "%d lignes météo insérées ou mises à jour (idempotent).", len(valid_rows)
    )
    return len(valid_rows)
