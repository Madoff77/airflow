"""
Journalisation applicative du pipeline TP5.

On expose un logger nommé pour chaque module. Airflow capture la sortie de
ces loggers et l'affiche dans les logs de tâche, ce qui donne des logs
applicatifs lisibles et préfixés par le nom du module.
"""

import logging


def get_logger(name):
    """
    Retourne un logger applicatif configuré pour Airflow.

    Le niveau est forcé à INFO pour garantir que les messages métier
    apparaissent dans les logs de tâche Airflow.
    """
    logger = logging.getLogger(f"tp5.{name}")
    logger.setLevel(logging.INFO)
    return logger
