# TP2 - Créer un premier DAG Airflow

## Description

Ce TP a pour objectif de créer un premier DAG Airflow simple et lisible.

Le DAG simule un pipeline météo en trois étapes :

1. récupérer des données météo fictives ;
2. vérifier que les données contiennent les champs obligatoires ;
3. simuler le chargement des données en base.

Le but est pédagogique : il n'y a pas d'appel API réel, pas de base de données métier réelle et pas de système d'alerte.

## Structure du projet

```text
.
├── config/
├── dags/
│   └── tp2_weather_dag.py
├── logs/
├── plugins/
├── docker-compose.yaml
└── README.md
```

Le fichier du DAG doit être placé dans :

```text
dags/tp2_weather_dag.py
```

## Lancer Airflow avec Docker Compose

Depuis la racine du projet, lancer :

```bash
docker compose up -d
```

Vérifier que le conteneur est démarré :

```bash
docker compose ps
```

Pour arrêter Airflow :

```bash
docker compose down
```

## Interface web Airflow

L'interface web est disponible à l'adresse :

```text
http://localhost:8080
```

Identifiants par défaut configurés dans `docker-compose.yaml` :

```text
Utilisateur : admin
Mot de passe : admin
```

## Vérifier que le DAG est détecté

Depuis la racine du projet :

```bash
docker compose exec airflow airflow dags list
```

Le DAG attendu doit apparaître avec l'identifiant :

```text
tp2_weather_pipeline
```

Il est aussi possible de vérifier ses tâches :

```bash
docker compose exec airflow airflow tasks list tp2_weather_pipeline
```

Résultat attendu :

```text
fetch_weather_data
validate_weather_data
load_weather_data
```

## Lancer le DAG manuellement

1. Ouvrir `http://localhost:8080`.
2. Se connecter avec `admin` / `admin`.
3. Chercher le DAG `tp2_weather_pipeline`.
4. Activer le DAG si nécessaire.
5. Cliquer sur le bouton de déclenchement manuel.
6. Attendre que les trois tâches passent en vert.

Le DAG ne se lance pas automatiquement, car il utilise :

```python
schedule=None
catchup=False
```

## Consulter les logs d'une tâche

Depuis l'interface Airflow :

1. ouvrir le DAG `tp2_weather_pipeline` ;
2. ouvrir l'exécution du DAG ;
3. cliquer sur une tâche, par exemple `fetch_weather_data` ;
4. cliquer sur `Logs`.

Les logs doivent afficher les messages `print` du DAG, par exemple les données météo simulées ou le message de validation réussie.

## Rôle des tâches

### `fetch_weather_data`

Cette tâche simule la récupération de données météo pour plusieurs villes.

Elle utilise une liste de dictionnaires Python :

```python
[
    {"city": "Paris", "temperature": 18, "humidity": 70},
    {"city": "Lyon", "temperature": 21, "humidity": 65},
    {"city": "Marseille", "temperature": 25, "humidity": 55},
]
```

Elle affiche les données récupérées dans les logs Airflow.

### `validate_weather_data`

Cette tâche vérifie que chaque donnée contient les champs obligatoires :

```text
city
temperature
humidity
```

Si un champ est manquant, elle lève une erreur claire avec `ValueError`.
Si les données sont complètes, elle affiche un message de validation réussie dans les logs.

### `load_weather_data`

Cette tâche simule le chargement des données météo en base.

Elle ne se connecte pas à une vraie base de données. Elle affiche simplement dans les logs que le chargement est terminé avec succès.

## Dépendances du DAG

Les dépendances sont définies explicitement dans le fichier Python :

```python
fetch_weather >> validate_weather >> load_weather
```

Cela signifie que :

1. `fetch_weather_data` s'exécute en premier ;
2. `validate_weather_data` s'exécute uniquement si la récupération a réussi ;
3. `load_weather_data` s'exécute uniquement si la validation a réussi.

## Preuve d'exécution attendue

Pour le compte rendu, il faut fournir :

1. une capture d'écran du DAG `tp2_weather_pipeline` avec les trois tâches en vert ;
2. une capture d'écran des logs d'une tâche, par exemple `fetch_weather_data` ou `validate_weather_data`.

## Texte court pour le compte rendu

Ce DAG Airflow simule un pipeline météo simple composé de trois tâches. La tâche `fetch_weather_data` crée des données météo fictives pour Paris, Lyon et Marseille. La tâche `validate_weather_data` vérifie que chaque donnée contient les champs obligatoires `city`, `temperature` et `humidity`. La tâche `load_weather_data` simule ensuite le chargement des données validées en base. Les dépendances sont définies explicitement avec `fetch_weather >> validate_weather >> load_weather`, ce qui rend l'ordre d'exécution clair. Le DAG utilise `schedule=None` et `catchup=False`, il ne se lance donc pas automatiquement et doit être déclenché manuellement depuis l'interface Airflow.
