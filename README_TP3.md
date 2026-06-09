# TP 3 - Préparer une ingestion API météo avec Open-Meteo

## Objectif

Ce TP continue le projet Airflow précédent avec un nouveau DAG qui appelle réellement l'API Open-Meteo.

Le DAG récupère des données météo pour trois villes, conserve uniquement les champs utiles, transforme les réponses JSON en structure exploitable, puis affiche un aperçu des données préparées dans les logs Airflow.

Le but reste pédagogique : il n'y a pas de vraie base de données métier et pas de système d'alerte.

## DAG créé

Fichier :

```text
dags/tp3_open_meteo_ingestion_dag.py
```

Identifiant du DAG dans Airflow :

```text
tp3_open_meteo_ingestion
```

Le DAG ne se lance pas automatiquement :

```python
schedule=None
catchup=False
```

Il utilise une politique de retry simple :

```text
2 retries
1 minute entre les tentatives
```

## Rôle des tâches

### `prepare_city_config`

Prépare la liste des villes à traiter avec leurs coordonnées GPS.

Villes utilisées :

```text
Paris
Lyon
Marseille
```

### `fetch_open_meteo_data`

Appelle réellement l'API Open-Meteo pour chaque ville.

Cette tâche récupère la réponse JSON brute et affiche dans les logs le statut de récupération pour chaque ville.

Elle vérifie aussi les erreurs HTTP ou réseau. En cas d'erreur, elle lève une exception claire avec le nom de la ville concernée.

### `transform_weather_data`

Transforme les réponses JSON brutes en données propres.

Cette tâche vérifie que la clé `current` existe et que les champs météo utiles sont présents.

Si un champ obligatoire manque, elle lève une `ValueError` claire.

### `preview_prepared_data`

Affiche dans les logs un aperçu des données préparées.

L'objectif est de montrer à quoi pourrait ressembler une future table cible, sans créer de vraie base de données.

## Dépendances du DAG

Les dépendances sont définies explicitement :

```python
prepare_city_config >> fetch_open_meteo_data >> transform_weather_data >> preview_prepared_data
```

Ordre d'exécution :

1. préparation des villes ;
2. appel de l'API Open-Meteo ;
3. transformation des données utiles ;
4. affichage de l'aperçu final.

## Champs retenus

La structure préparée contient uniquement les champs suivants :

```python
{
    "city": "...",
    "latitude": ...,
    "longitude": ...,
    "observation_time": "...",
    "temperature_celsius": ...,
    "humidity_percent": ...,
    "wind_speed_kmh": ...,
    "weather_code": ...,
    "ingestion_date": "..."
}
```

Justification des champs :

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

## Lancer Airflow

Depuis la racine du projet :

```bash
docker compose up
```

Pour lancer Airflow en arrière-plan :

```bash
docker compose up -d
```

Vérifier que le conteneur est démarré :

```bash
docker compose ps
```

Interface web Airflow :

```text
http://localhost:8080
```

Identifiants configurés dans ce projet :

```text
Utilisateur : admin
Mot de passe : admin
```

## Vérifier que le DAG est détecté

Depuis la racine du projet :

```bash
docker compose exec airflow airflow dags list
```

Le DAG attendu doit apparaître :

```text
tp3_open_meteo_ingestion
```

Vérifier les tâches :

```bash
docker compose exec airflow airflow tasks list tp3_open_meteo_ingestion
```

Résultat attendu :

```text
prepare_city_config
fetch_open_meteo_data
transform_weather_data
preview_prepared_data
```

## Lancer le DAG manuellement

1. Ouvrir `http://localhost:8080`.
2. Se connecter avec `admin` / `admin`.
3. Chercher le DAG `tp3_open_meteo_ingestion`.
4. Activer le DAG si nécessaire.
5. Cliquer sur le bouton de déclenchement manuel.
6. Attendre que les quatre tâches passent en vert.

Le conteneur doit avoir accès à Internet pour appeler l'API Open-Meteo.

## Consulter les logs

Depuis l'interface Airflow :

1. ouvrir le DAG `tp3_open_meteo_ingestion` ;
2. ouvrir l'exécution du DAG ;
3. cliquer sur une tâche, par exemple `preview_prepared_data` ;
4. cliquer sur `Logs`.

Les logs doivent afficher l'aperçu des données préparées pour la future table cible.

## Preuve d'exécution attendue

Pour le compte rendu, faire :

1. une capture du DAG `tp3_open_meteo_ingestion` avec les quatre tâches en vert ;
2. une capture des logs de `preview_prepared_data` montrant les données préparées ;
3. éventuellement une capture des logs de `fetch_open_meteo_data` montrant les appels réussis pour Paris, Lyon et Marseille.

## Texte court pour le compte rendu

Ce DAG Airflow appelle l'API Open-Meteo pour plusieurs villes : Paris, Lyon et Marseille. Il récupère d'abord le JSON brut renvoyé par l'API, puis transforme uniquement les champs utiles pour le besoin métier. Les données préparées contiennent la ville, les coordonnées GPS, l'heure d'observation, la température, l'humidité, la vitesse du vent, le code météo et la date d'ingestion. Le DAG sépare clairement la récupération API, la transformation et l'affichage final. La dernière tâche affiche une structure prête à être stockée dans une future table cible.
