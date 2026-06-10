# TP 5 — Industrialisation d'un pipeline Airflow Open-Meteo

## 1. Description du pipeline

Ce TP fait évoluer le pipeline Open-Meteo des TP précédents vers une version
**industrialisée** : code organisé en modules Python séparés, archivage des
données brutes, contrôle qualité explicite, branchement conditionnel, gestion
des erreurs, idempotence, logs applicatifs et traçabilité d'ingestion.

Le DAG traite plusieurs villes configurables. À chaque exécution il :

1. prépare la liste des villes ;
2. récupère les données météo depuis l'API Open-Meteo ;
3. archive le JSON brut sur disque ;
4. transforme le JSON en lignes propres ;
5. crée les tables PostgreSQL si nécessaire ;
6. exécute des contrôles qualité ;
7. **décide** s'il peut charger les données (branchement conditionnel) ;
8. charge uniquement les données valides, **ou** trace l'anomalie sans charger ;
9. écrit une ligne de traçabilité d'ingestion.

Le DAG ne charge **jamais** de données jugées non valides : en cas d'anomalie
qualité, la branche de chargement est ignorée et l'anomalie est tracée.

Le TP5 ne supprime aucun DAG précédent (TP2, TP3, TP4 restent en place).

Identifiant du DAG :

```text
tp5_open_meteo_industrial
```

## 2. Structuration du projet

Le TP5 sépare l'**orchestration** (le fichier DAG) de la **logique métier**
(les modules du package `tp5`).

```text
dags/
├── tp5_open_meteo_industrial_dag.py   # orchestration uniquement
└── tp5/
    ├── __init__.py
    ├── config.py          # constantes et paramètres centralisés
    ├── logging_utils.py   # logger applicatif homogène
    ├── extract.py         # préparation villes + appel API Open-Meteo
    ├── archive.py         # archivage des données brutes
    ├── transform.py       # transformation JSON -> lignes
    ├── quality.py         # contrôles qualité + décision de branchement
    ├── load.py            # création des tables + chargement idempotent
    └── tracking.py        # traçabilité des exécutions
sql/
└── 002_create_tp5_tables.sql          # schéma des tables TP5
data/raw/                              # archives brutes (générées au run)
proofs/                                # captures de preuves (à remplir au run)
README_TP5.md
```

Le dossier `dags/` est ajouté au `PYTHONPATH` par Airflow, ce qui permet les
imports `from tp5.config import ...` depuis le fichier DAG.

## 3. Schéma logique du workflow

```text
        prepare_city_config
                │
        fetch_open_meteo_data        (appel API, timeout + retries)
                │
        archive_raw_data             (JSON brut écrit sur disque)
                │
        transform_weather_data       (JSON -> lignes normalisées)
                │
        create_tables                (CREATE TABLE IF NOT EXISTS)
                │
        run_quality_checks           (produit un rapport qualité)
                │
        decide_branch                (BRANCHEMENT CONDITIONNEL)
            ┌───┴───────────────────────────┐
       qualité OK                      anomalie détectée
            │                                │
     load_valid_data               trace_quality_anomaly
            │                       (aucun chargement)
     trace_success                          │
            │                                │
            └────────────► end ◄─────────────┘
                  (NONE_FAILED_MIN_ONE_SUCCESS)
```

Dépendances déclarées dans le code :

```python
prepare_city_config >> fetch_open_meteo_data >> archive_raw_data
archive_raw_data >> transform_weather_data >> create_tables
create_tables >> run_quality_checks >> decide_branch

decide_branch >> load_valid_data >> trace_success >> end   # branche conforme
decide_branch >> trace_quality_anomaly >> end              # branche anomalie
```

## 4. Variables Airflow utilisées

Le pipeline ne dépend pas de Variables Airflow stockées en base : les
paramètres sont centralisés dans `dags/tp5/config.py` (villes par défaut,
seuils qualité, noms de tables, chemins). Ce choix rend le projet
auto-portant et reproductible.

La seule entrée dynamique passe par la **configuration d'exécution**
(`dag_run.conf`) au lancement manuel :

| Clé de `dag_run.conf` | Type | Rôle |
| --- | --- | --- |
| `cities` | liste d'objets | Surcharge la liste des villes par défaut. |
| `simulate_quality_anomaly` | booléen | Injecte une valeur impossible pour démontrer le cas d'anomalie qualité. |

Exemple de surcharge des villes :

```json
{
  "cities": [
    {"city": "Alger", "latitude": 36.7538, "longitude": 3.0588},
    {"city": "Oran", "latitude": 35.6971, "longitude": -0.6308}
  ]
}
```

## 5. Connexions Airflow utilisées

| Connexion | Usage |
| --- | --- |
| `weather_postgres` | Connexion PostgreSQL vers la base métier `weather`. |

Cette connexion est déclarée dans `docker-compose.yaml` via la variable
d'environnement :

```text
AIRFLOW_CONN_WEATHER_POSTGRES=postgresql://weather:weather@weather-postgres:5432/weather
```

## 6. Description des tâches du DAG

| Tâche | Module | Rôle |
| --- | --- | --- |
| `prepare_city_config` | `extract.py` | Prépare la liste des villes (défaut ou `dag_run.conf`). |
| `fetch_open_meteo_data` | `extract.py` | Appelle l'API Open-Meteo pour chaque ville (timeout, retries). |
| `archive_raw_data` | `archive.py` | Archive le JSON brut dans `data/raw/<run_id>/<ville>.json`. |
| `transform_weather_data` | `transform.py` | Normalise le JSON en lignes prêtes pour PostgreSQL. |
| `create_tables` | `load.py` | Crée les tables TP5 si elles n'existent pas. |
| `run_quality_checks` | `quality.py` | Contrôle chaque ligne et publie un rapport qualité. |
| `decide_branch` | `quality.py` | **Branchement conditionnel** selon le rapport qualité. |
| `load_valid_data` | `load.py` | Charge les lignes valides (idempotent). Branche conforme. |
| `trace_success` | `tracking.py` | Trace l'exécution réussie. Branche conforme. |
| `trace_quality_anomaly` | `tracking.py` | Trace l'anomalie sans charger. Branche anomalie. |
| `end` | DAG | Point de convergence des deux branches. |

## 7. Stratégie de robustesse

- **Retries et retry delay** : `retries=2` et `retry_delay=1 min` sur toutes
  les tâches (`default_args`). Une panne transitoire de l'API est réessayée.
- **Timeout** : `execution_timeout=5 min` par défaut, réduit à `3 min` sur
  l'appel API externe `fetch_open_meteo_data` (garde-fou réseau).
- **Gestion des erreurs** : l'appel API distingue erreurs HTTP, erreurs réseau
  et JSON invalide, et lève des exceptions explicites. La transformation
  vérifie la présence de la clé `current` et des champs obligatoires.
- **`max_active_runs=1`** : évite deux exécutions concurrentes sur les mêmes
  tables.
- **Séparation des responsabilités** : chaque module a un rôle unique, ce qui
  facilite la lecture, le test et la maintenance.
- **Données brutes archivées** : on peut auditer ou rejouer une exécution sans
  rappeler l'API.

## 8. Stratégie d'idempotence

L'idempotence est garantie à deux niveaux :

1. **Table métier `tp5_weather_observations`** : contrainte
   `UNIQUE (city, observation_time)` + clause `ON CONFLICT ... DO UPDATE`.
   Relancer le DAG sur la même observation **met à jour** la ligne au lieu de
   créer un doublon.
2. **Table de suivi `tp5_ingestion_tracking`** : contrainte
   `UNIQUE (dag_id, run_id)` + `ON CONFLICT ... DO UPDATE`. Relancer le même
   run met à jour la ligne de suivi existante.
3. **Archivage** : les fichiers bruts sont rangés par `run_id`. Rejouer le même
   run écrase les mêmes fichiers, sans accumulation.

Résultat : une relance ne crée aucun doublon, ni en base, ni sur disque.

## 9. Contrôles qualité mis en place

Le module `quality.py` applique, pour chaque ligne :

- **Champs obligatoires** présents et non nuls : `city`, `observation_time`,
  `temperature_celsius`, `humidity_percent`, `wind_speed_kmh`, `weather_code`.
- **Bornes physiques plausibles** :
  - température entre `-90` et `60` °C ;
  - humidité entre `0` et `100` % ;
  - vitesse du vent entre `0` et `500` km/h.

Toute violation produit une **anomalie** (ville + raison) dans le rapport
qualité. Le contrôle ne fait pas échouer la tâche : il produit un rapport, qui
pilote ensuite le branchement.

## 10. Règle de branchement conditionnel

La tâche `decide_branch` (un `BranchPythonOperator`) lit le rapport qualité :

- **statut `ok`** (aucune anomalie) → exécute `load_valid_data` puis
  `trace_success` ;
- **statut `anomaly`** (au moins une anomalie) → exécute
  `trace_quality_anomaly`, **sans aucun chargement**.

La tâche `end` utilise `trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS` pour réussir
dès qu'une des deux branches aboutit, sans propager l'état « skipped ».

## 11. Description des logs produits

Chaque module utilise un logger applicatif nommé (`tp5.extract`,
`tp5.quality`, …) via `logging_utils.get_logger`. Airflow capture ces loggers
et les affiche dans les logs de tâche. On y trouve notamment :

- la ville préparée et appelée ;
- la confirmation d'archivage et le chemin du fichier brut ;
- le nombre de lignes transformées ;
- pour chaque ligne : `Ligne valide` ou `Anomalie qualité ... : <raison>` ;
- le résumé du contrôle qualité (reçues / valides / anomalies) ;
- la décision de branchement ;
- le nombre de lignes chargées, ou le message d'anomalie tracée.

## 12. Description des tables PostgreSQL

### `tp5_weather_observations` (table métier)

| Colonne | Type | Rôle |
| --- | --- | --- |
| `city` | VARCHAR | Ville concernée. |
| `latitude`, `longitude` | DOUBLE | Coordonnées de la source. |
| `observation_time` | TIMESTAMP | Heure de la mesure. |
| `temperature_celsius` | DOUBLE | Température. |
| `humidity_percent` | DOUBLE | Humidité. |
| `wind_speed_kmh` | DOUBLE | Vitesse du vent. |
| `weather_code` | INTEGER | Code météo. |
| `ingestion_date` | TIMESTAMP | Date d'ingestion. |

Contrainte d'idempotence : `UNIQUE (city, observation_time)`.

### `tp5_ingestion_tracking` (table de traçabilité)

| Colonne | Type | Rôle |
| --- | --- | --- |
| `dag_id`, `run_id` | VARCHAR | Identifient l'exécution. |
| `ingestion_date` | TIMESTAMP | Date d'écriture du suivi. |
| `cities_count` | INTEGER | Nombre de villes traitées. |
| `rows_received` | INTEGER | Lignes reçues du contrôle qualité. |
| `rows_loaded` | INTEGER | Lignes réellement chargées (0 si anomalie). |
| `quality_status` | VARCHAR | `success` ou `quality_failed`. |
| `anomalies_count` | INTEGER | Nombre d'anomalies détectées. |
| `message` | TEXT | Détail lisible de l'exécution. |

Contrainte d'idempotence : `UNIQUE (dag_id, run_id)`.

## 13. Lancer le projet

Depuis la racine du projet :

```bash
docker compose up -d
docker compose ps
```

Interface Airflow : `http://localhost:8080` (identifiants `admin` / `admin`).

Vérifier que le DAG est détecté :

```bash
docker compose exec airflow airflow dags list | grep tp5
docker compose exec airflow airflow tasks list tp5_open_meteo_industrial --tree
```

## 14. Démonstration des cas attendus

### 14.1 Cas nominal

1. Ouvrir Airflow, activer `tp5_open_meteo_industrial`.
2. Déclencher le DAG **sans configuration** (villes par défaut).
3. Toutes les tâches passent au vert ; la branche `load_valid_data` →
   `trace_success` est exécutée, `trace_quality_anomaly` est `skipped`.

Vérifier le chargement :

```bash
docker compose exec weather-postgres psql -U weather -d weather -c \
  "SELECT city, observation_time, temperature_celsius, humidity_percent, wind_speed_kmh, weather_code FROM tp5_weather_observations ORDER BY city;"

docker compose exec weather-postgres psql -U weather -d weather -c \
  "SELECT run_id, quality_status, rows_received, rows_loaded, anomalies_count, message FROM tp5_ingestion_tracking ORDER BY id DESC;"
```

**Preuves à capturer** : DAG en vert (branche conforme), logs de
`load_valid_data`, contenu de `tp5_weather_observations`, ligne `success` dans
`tp5_ingestion_tracking`.

### 14.2 Cas d'anomalie qualité

1. Déclencher le DAG avec la configuration :

   ```json
   {"simulate_quality_anomaly": true}
   ```

2. La transformation injecte une température impossible (`999.0`).
3. `run_quality_checks` détecte l'anomalie, `decide_branch` route vers
   `trace_quality_anomaly`. La tâche `load_valid_data` est `skipped` : **aucune
   donnée n'est chargée**.

Vérifier la traçabilité de l'anomalie :

```bash
docker compose exec weather-postgres psql -U weather -d weather -c \
  "SELECT run_id, quality_status, rows_loaded, anomalies_count, message FROM tp5_ingestion_tracking WHERE quality_status='quality_failed' ORDER BY id DESC;"
```

**Preuves à capturer** : DAG avec `load_valid_data` en `skipped` et
`trace_quality_anomaly` en vert, logs de `run_quality_checks` montrant
l'anomalie, ligne `quality_failed` dans `tp5_ingestion_tracking`, et absence de
nouvelle ligne dans `tp5_weather_observations` pour ce run.

### 14.3 Cas de relance (idempotence)

1. Relancer le DAG nominal **une seconde fois** (mêmes villes).
2. Compter les lignes avant / après :

   ```bash
   docker compose exec weather-postgres psql -U weather -d weather -c \
     "SELECT city, COUNT(*) FROM tp5_weather_observations GROUP BY city ORDER BY city;"
   ```

Le nombre de lignes par ville **ne doit pas augmenter** : la clause
`ON CONFLICT` met à jour les lignes existantes au lieu de les dupliquer.

**Preuve à capturer** : le `COUNT(*)` identique avant et après la relance.

## 15. Livrables et emplacement des preuves

| Livrable | Emplacement |
| --- | --- |
| Code du DAG | `dags/tp5_open_meteo_industrial_dag.py` |
| Modules Python séparés | `dags/tp5/*.py` |
| Scripts SQL | `sql/002_create_tp5_tables.sql` |
| README | `README_TP5.md` |
| Schéma logique | section 3 de ce README |
| Preuves d'exécution | dossier `proofs/` (voir `proofs/README.md`) |

## 16. Limites éventuelles du travail rendu

- **Exécuteur séquentiel** : le projet utilise `SequentialExecutor` avec
  métadonnées SQLite (héritage des TP précédents). Les tâches s'exécutent une à
  une ; il n'y a pas de parallélisme entre villes.
- **API temps réel** : Open-Meteo ne renvoie que la météo courante. Deux runs
  rapprochés peuvent renvoyer le même `observation_time` ; c'est précisément ce
  qui permet de démontrer l'idempotence, mais cela limite l'historisation fine.
- **Contrôle qualité par bornes** : les seuils sont des garde-fous physiques
  simples. Un contrôle plus poussé (cohérence temporelle, valeurs aberrantes
  statistiques) dépasse le périmètre du TP.
- **Anomalie simulée** : le cas d'anomalie est déclenché par un drapeau de
  configuration qui injecte une valeur impossible. C'est volontaire et sûr :
  cela exerce réellement le chemin de détection sans corrompre l'API.
- **Preuves d'exécution** : les captures (DAG, logs, tables) sont produites au
  lancement sur la machine de l'évaluateur, en suivant la section 14.
```

