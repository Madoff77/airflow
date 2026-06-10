"""
Configuration centralisée du pipeline TP5.

Toutes les constantes du pipeline sont regroupées ici afin de garder les
modules métier lisibles et de pouvoir ajuster un paramètre à un seul endroit.
"""

from pathlib import Path

# --- API Open-Meteo --------------------------------------------------------
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
HTTP_TIMEOUT_SECONDS = 30
USER_AGENT = "airflow-tp5-open-meteo"

# Champs météo demandés à l'API (bloc "current").
CURRENT_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "weather_code",
]

# --- PostgreSQL ------------------------------------------------------------
# Connexion Airflow déclarée dans docker-compose.yaml
# (AIRFLOW_CONN_WEATHER_POSTGRES).
POSTGRES_CONN_ID = "weather_postgres"

# Script SQL de création des tables, monté dans le conteneur.
CREATE_TABLES_SQL_PATH = Path("/opt/airflow/sql/002_create_tp5_tables.sql")

# Tables métier du TP5 (préfixées tp5_ pour ne pas écraser celles du TP4).
WEATHER_TABLE = "tp5_weather_observations"
TRACKING_TABLE = "tp5_ingestion_tracking"

# --- Archivage des données brutes ------------------------------------------
# Répertoire monté via le volume ./data:/opt/airflow/data du docker-compose.
RAW_ARCHIVE_DIR = Path("/opt/airflow/data/raw")

# --- Villes par défaut -----------------------------------------------------
# Surchargables au lancement manuel via dag_run.conf["cities"].
DEFAULT_CITIES = [
    {"city": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Lyon", "latitude": 45.7640, "longitude": 4.8357},
    {"city": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]

# --- Seuils de contrôle qualité --------------------------------------------
# Bornes physiques plausibles. Toute valeur hors borne est une anomalie.
QUALITY_RULES = {
    "temperature_celsius": {"min": -90.0, "max": 60.0},
    "humidity_percent": {"min": 0.0, "max": 100.0},
    "wind_speed_kmh": {"min": 0.0, "max": 500.0},
}

# Champs obligatoires d'une ligne transformée (ne doivent jamais être nuls).
REQUIRED_ROW_FIELDS = [
    "city",
    "observation_time",
    "temperature_celsius",
    "humidity_percent",
    "wind_speed_kmh",
    "weather_code",
]

# --- Identifiants des tâches (cibles du branchement conditionnel) ----------
TASK_LOAD_VALID_DATA = "load_valid_data"
TASK_TRACE_QUALITY_ANOMALY = "trace_quality_anomaly"
