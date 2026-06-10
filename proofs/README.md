# Preuves d'exécution — TP5

Déposer ici les captures demandées par le sujet. Procédure détaillée :
section 14 de `README_TP5.md`.

Captures attendues :

## Cas nominal
- `nominal_dag_green.png` — graphe du DAG, branche conforme en vert
  (`load_valid_data` et `trace_success` réussies).
- `nominal_logs_load.png` — logs de la tâche `load_valid_data`.
- `nominal_table_observations.png` — `SELECT` sur `tp5_weather_observations`.
- `nominal_table_tracking.png` — ligne `success` dans `tp5_ingestion_tracking`.

## Cas d'anomalie qualité
- `anomaly_dag_branch.png` — DAG avec `load_valid_data` en `skipped` et
  `trace_quality_anomaly` en vert.
- `anomaly_logs_quality.png` — logs de `run_quality_checks` montrant l'anomalie.
- `anomaly_table_tracking.png` — ligne `quality_failed` dans
  `tp5_ingestion_tracking`.

## Cas de relance (idempotence)
- `rerun_count_before.png` / `rerun_count_after.png` — `COUNT(*)` par ville
  identique avant et après relance.

## Logs Airflow
- `airflow_logs.png` — exemple de logs applicatifs d'une tâche.
