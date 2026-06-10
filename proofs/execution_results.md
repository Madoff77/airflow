# Preuves d'exécution TP5 — sorties réelles

Exécutions lancées le 2026-06-10 sur le DAG `tp5_open_meteo_industrial`
(Airflow 2.9.3, SequentialExecutor). Ces sorties texte complètent les
captures d'écran de l'UI Airflow à déposer dans ce dossier.

## 1. DAG détecté, sans erreur d'import

```text
$ airflow dags list | grep tp5
tp5_open_meteo_industrial  | /opt/airflow/dags/tp5_open_meteo_industrial_dag.py  | student | True

$ airflow dags list-import-errors
No data found
```

Arborescence des tâches :

```text
prepare_city_config
  fetch_open_meteo_data
    archive_raw_data
      transform_weather_data
        create_tables
          run_quality_checks
            decide_branch
              load_valid_data
                trace_success
                  end
              trace_quality_anomaly
                end
```

## 2. Cas nominal (run_id = nominal_001) — SUCCÈS

Toutes les tâches de la branche conforme réussies. Données chargées :

```text
   city    |  observation_time   | temperature_celsius | humidity_percent | wind_speed_kmh | weather_code
-----------+---------------------+---------------------+------------------+----------------+--------------
 Lyon      | 2026-06-10 12:45:00 |                17.8 |               51 |           14.7 |            3
 Marseille | 2026-06-10 12:45:00 |                23.3 |               36 |           33.9 |            3
 Paris     | 2026-06-10 12:45:00 |                17.3 |               55 |           13.7 |            3
```

Traçabilité :

```text
   run_id    | quality_status | rows_received | rows_loaded | anomalies_count
-------------+----------------+---------------+-------------+-----------------
 nominal_001 | success        |             3 |           3 |               0
```

## 3. Cas d'anomalie qualité (run_id = anomaly_001, conf simulate_quality_anomaly=true)

États des tâches — la branche de chargement est ignorée :

```text
task_id                | state
-----------------------+---------
prepare_city_config    | success
fetch_open_meteo_data  | success
archive_raw_data       | success
transform_weather_data | success
create_tables          | success
run_quality_checks     | success
decide_branch          | success
load_valid_data        | skipped    <-- chargement bloqué
trace_success          | skipped
trace_quality_anomaly  | success    <-- anomalie tracée
end                    | success
```

Logs applicatifs du contrôle qualité :

```text
{quality.py:63} ERROR - Anomalie qualité pour Paris : temperature_celsius=999.0 hors bornes [-90.0, 60.0]
{quality.py:70} INFO  - Ligne valide pour Lyon.
{quality.py:70} INFO  - Ligne valide pour Marseille.
{quality.py:80} INFO  - Contrôle qualité terminé : statut=anomaly, reçues=3, valides=2, anomalies=1.
```

Traçabilité de l'anomalie (rows_loaded = 0) :

```text
   run_id    | quality_status | rows_received | rows_loaded | anomalies_count |                  message
-------------+----------------+---------------+-------------+-----------------+-------------------------------------------
 anomaly_001 | quality_failed |             3 |           0 |               1 | Anomalie qualité : chargement bloqué.
                                                                                Détail: Paris: temperature_celsius=999.0
                                                                                hors bornes [-90.0, 60.0]
```

Vérification : aucune donnée corrompue chargée (Paris reste à 17.3 °C, pas 999).

## 4. Cas de relance / idempotence (run_id = idem_002)

Relance dans la même fenêtre d'observation (13:00). Compte par ville
**inchangé** malgré la relance — `ON CONFLICT DO UPDATE` met à jour sans
dupliquer :

```text
COUNT avant relance = 6
   city    | count
-----------+-------
 Lyon      |     2
 Marseille |     2
 Paris     |     2
COUNT après relance = 6   (identique : aucun doublon)
```

> Remarque : les 6 lignes correspondent à 2 fenêtres d'observation distinctes
> (12:45 et 13:00), car l'API Open-Meteo avance son horodatage toutes les
> 15 minutes. Deux runs dans la **même** fenêtre ne créent donc bien aucun
> doublon (clé d'unicité `(city, observation_time)`).

## 5. Archivage des données brutes

Un fichier JSON brut par ville et par run, rangé par `run_id` :

```text
/opt/airflow/data/raw/
├── nominal_001/  {Lyon,Marseille,Paris}.json
├── anomaly_001/  {Lyon,Marseille,Paris}.json
├── rerun_001/    {Lyon,Marseille,Paris}.json
└── idem_002/     {Lyon,Marseille,Paris}.json
```

## 6. Synthèse de la traçabilité (toutes exécutions)

```text
   run_id    | quality_status | rows_received | rows_loaded | anomalies_count
-------------+----------------+---------------+-------------+-----------------
 nominal_001 | success        |             3 |           3 |               0
 anomaly_001 | quality_failed |             3 |           0 |               1
 rerun_001   | success        |             3 |           3 |               0
 idem_002    | success        |             3 |           3 |               0
```
