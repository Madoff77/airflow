# TP 4 - Pipeline complet API -> transformation -> PostgreSQL

## Objectif

Ce TP ajoute un pipeline Airflow complet à partir de l'API Open-Meteo.

Le DAG récupère les données météo, transforme le JSON brut en structure exploitable, charge les lignes dans PostgreSQL, puis écrit une ligne de suivi d'ingestion.

Le TP4 continue le TP3 sans supprimer les DAGs précédents.

## Fichiers du TP

```text
dags/tp4_open_meteo_to_postgres_dag.py
sql/001_create_weather_tables.sql
README_TP4.md
```

Identifiant du DAG Airflow :

```text
tp4_open_meteo_to_postgres
```

## Rôle des tâches

### `prepare_city_config`

Prépare la liste des villes à traiter.

Par défaut, le DAG traite :

```text
Paris
Lyon
Marseille
```

La liste peut être surchargée au lancement manuel avec `dag_run.conf`.

### `fetch_open_meteo_data`

Appelle réellement l'API Open-Meteo pour chaque ville.

Cette tâche récupère le JSON brut et loggue la ville concernée. En cas d'erreur HTTP ou réseau, elle lève une exception claire.

### `transform_weather_data`

Transforme le JSON brut en lignes propres, cohérentes avec la future table PostgreSQL.

Elle vérifie la présence de la clé `current` et des champs obligatoires.

### `create_weather_tables`

Crée les tables PostgreSQL si elles n'existent pas.

Cette tâche utilise le script :

```text
sql/001_create_weather_tables.sql
```

### `load_weather_data_to_postgres`

Insère les données météo transformées dans PostgreSQL.

La table possède une contrainte unique sur `(city, observation_time)`. Le DAG utilise `ON CONFLICT` pour éviter les doublons et mettre à jour une ligne déjà existante.

### `write_ingestion_tracking`

Écrit une ligne dans la table de suivi `ingestion_tracking`.

Cette ligne contient le DAG, le run Airflow, la date d'ingestion, le nombre de villes, le nombre de lignes préparées, le statut et un message court.

## Schéma logique du DAG

```text
prepare_city_config
  ↓
fetch_open_meteo_data
  ↓
transform_weather_data
  ↓
create_weather_tables
  ↓
load_weather_data_to_postgres
  ↓
write_ingestion_tracking
```

Dépendances dans le code :

```python
prepare_city_config >> fetch_open_meteo_data >> transform_weather_data
transform_weather_data >> create_weather_tables >> load_weather_data_to_postgres >> write_ingestion_tracking
```

## Tables PostgreSQL créées

### `weather_observations`

Table métier contenant les observations météo préparées.

Colonnes principales :

```text
city
latitude
longitude
observation_time
temperature_celsius
humidity_percent
wind_speed_kmh
weather_code
ingestion_date
```

Une contrainte unique évite les doublons :

```text
UNIQUE (city, observation_time)
```

### `ingestion_tracking`

Table de suivi des exécutions du pipeline.

Colonnes principales :

```text
dag_id
run_id
ingestion_date
cities_count
rows_count
status
message
```

## Champs retenus et justification

| Champ | Justification |
| --- | --- |
| `city` | Identifie la ville concernée. |
| `latitude` | Trace la source géographique exacte. |
| `longitude` | Trace la source géographique exacte. |
| `observation_time` | Indique le moment de la mesure météo. |
| `temperature_celsius` | Donne l'indicateur météo principal. |
| `humidity_percent` | Permet d'analyser les conditions atmosphériques. |
| `wind_speed_kmh` | Sert au suivi météo opérationnel. |
| `weather_code` | Permet de classifier l'état météo. |
| `ingestion_date` | Trace la date d'ingestion dans le pipeline. |

## Lancer Airflow et PostgreSQL

Depuis la racine du projet :

```bash
docker compose up -d
```

Vérifier les conteneurs :

```bash
docker compose ps
```

Interface Airflow :

```text
http://localhost:8080
```

Identifiants Airflow configurés dans ce projet :

```text
Utilisateur : admin
Mot de passe : admin
```

Connexion PostgreSQL configurée pour Airflow :

```text
weather_postgres
```

Cette connexion est déclarée dans `docker-compose.yaml` avec :

```text
AIRFLOW_CONN_WEATHER_POSTGRES=postgresql://weather:weather@weather-postgres:5432/weather
```

## Vérifier que le DAG est détecté

```bash
docker compose exec airflow airflow dags list
```

Le DAG attendu doit apparaître :

```text
tp4_open_meteo_to_postgres
```

Vérifier les tâches :

```bash
docker compose exec airflow airflow tasks list tp4_open_meteo_to_postgres --tree
```

## Lancer le DAG manuellement

1. Ouvrir `http://localhost:8080`.
2. Se connecter avec `admin` / `admin`.
3. Chercher `tp4_open_meteo_to_postgres`.
4. Activer le DAG si nécessaire.
5. Cliquer sur le bouton de lancement manuel.
6. Attendre que toutes les tâches passent en vert.

Le DAG ne se lance pas automatiquement :

```python
schedule=None
catchup=False
```

## Passer des villes personnalisées

Au lancement manuel, il est possible de fournir une configuration JSON.

Exemple :

```json
{
  "cities": [
    {"city": "Alger", "latitude": 36.7538, "longitude": 3.0588},
    {"city": "Oran", "latitude": 35.6971, "longitude": -0.6308}
  ]
}
```

Si aucune configuration n'est fournie, le DAG utilise Paris, Lyon et Marseille.

## Vérifier le chargement dans PostgreSQL

Ouvrir un terminal PostgreSQL :

```bash
docker compose exec weather-postgres psql -U weather -d weather
```

Afficher les observations météo :

```sql
SELECT
    city,
    observation_time,
    temperature_celsius,
    humidity_percent,
    wind_speed_kmh,
    weather_code,
    ingestion_date
FROM weather_observations
ORDER BY city, observation_time;
```

Afficher le suivi d'ingestion :

```sql
SELECT
    dag_id,
    run_id,
    ingestion_date,
    cities_count,
    rows_count,
    status,
    message
FROM ingestion_tracking
ORDER BY id DESC;
```

Commandes directes sans entrer dans `psql` :

```bash
docker compose exec weather-postgres psql -U weather -d weather -c "SELECT city, observation_time, temperature_celsius, humidity_percent, wind_speed_kmh, weather_code FROM weather_observations ORDER BY city;"
docker compose exec weather-postgres psql -U weather -d weather -c "SELECT dag_id, run_id, cities_count, rows_count, status, message FROM ingestion_tracking ORDER BY id DESC;"
```

## Preuve d'exécution attendue

Pour le compte rendu, fournir :

1. une capture du DAG Airflow avec les tâches en vert ;
2. une capture des logs de `load_weather_data_to_postgres` ;
3. une capture ou copie du résultat SQL montrant les lignes dans `weather_observations` ;
4. une capture ou copie du résultat SQL montrant une ligne dans `ingestion_tracking`.

## Texte court pour le compte rendu

Ce DAG Airflow récupère les données météo depuis l'API Open-Meteo pour plusieurs villes. Il transforme le JSON brut en structure exploitable en conservant uniquement les champs utiles : ville, coordonnées, heure d'observation, température, humidité, vitesse du vent, code météo et date d'ingestion. Les données préparées sont ensuite chargées dans une table PostgreSQL `weather_observations`, avec une contrainte pour éviter les doublons. Enfin, le DAG écrit une ligne dans `ingestion_tracking` afin de tracer l'exécution du pipeline, le nombre de villes traitées et le nombre de lignes chargées.
