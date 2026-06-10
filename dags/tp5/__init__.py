"""
Package TP5 - Pipeline Airflow Open-Meteo industrialisé.

Ce package regroupe les modules métier du pipeline TP5 :

- ``config``        : constantes et paramètres centralisés ;
- ``logging_utils`` : journalisation applicative homogène ;
- ``extract``       : extraction depuis l'API Open-Meteo ;
- ``archive``       : archivage des données brutes sur disque ;
- ``transform``     : transformation du JSON brut en lignes propres ;
- ``quality``       : contrôles qualité et décision de branchement ;
- ``load``          : chargement idempotent dans PostgreSQL ;
- ``tracking``      : traçabilité des exécutions d'ingestion.

Le DAG ``tp5_open_meteo_industrial`` se contente d'orchestrer ces modules.
"""
