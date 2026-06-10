"""
Archivage des données brutes Open-Meteo.

Avant toute transformation, on conserve le JSON brut tel que renvoyé par
l'API. Cela permet de rejouer ou d'auditer une exécution sans rappeler l'API.

L'archivage est idempotent : les fichiers sont rangés dans un dossier nommé
d'après le run_id Airflow. Relancer le même run écrase les mêmes fichiers,
sans créer de doublon.
"""

import json
import re

from tp5 import config
from tp5.logging_utils import get_logger

logger = get_logger("archive")


def _safe_name(value):
    """Nettoie une chaîne pour en faire un nom de fichier sûr."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", str(value)).strip("_")


def archive_raw_data(ti, **context):
    """
    Écrit le JSON brut de chaque ville dans le répertoire d'archive.

    Renvoie la liste des chemins archivés (utile pour la traçabilité).
    """
    raw_weather_data = ti.xcom_pull(task_ids="fetch_open_meteo_data")

    if not raw_weather_data:
        raise ValueError("Aucune donnée brute à archiver.")

    run_id = context["run_id"]
    run_dir = config.RAW_ARCHIVE_DIR / _safe_name(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    archived_paths = []

    for raw_city_data in raw_weather_data:
        city_name = raw_city_data["city"]
        file_path = run_dir / f"{_safe_name(city_name)}.json"

        # Écriture déterministe : même run + même ville => même fichier écrasé.
        file_path.write_text(
            json.dumps(raw_city_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        archived_paths.append(str(file_path))
        logger.info("Données brutes archivées pour %s dans %s.", city_name, file_path)

    logger.info("%d fichiers bruts archivés dans %s.", len(archived_paths), run_dir)
    return archived_paths
