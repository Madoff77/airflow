-- Tables du TP5 : pipeline Open-Meteo industrialisé.
-- Préfixe tp5_ pour ne pas écraser les tables du TP4.

-- Table métier : observations météo valides.
CREATE TABLE IF NOT EXISTS tp5_weather_observations (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    observation_time TIMESTAMP NOT NULL,
    temperature_celsius DOUBLE PRECISION,
    humidity_percent DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    weather_code INTEGER,
    ingestion_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Clé d'idempotence : une seule ligne par (ville, heure d'observation).
    UNIQUE (city, observation_time)
);

-- Table de traçabilité : une ligne de suivi par exécution du DAG.
CREATE TABLE IF NOT EXISTS tp5_ingestion_tracking (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    ingestion_date TIMESTAMP NOT NULL,
    cities_count INTEGER NOT NULL,
    rows_received INTEGER NOT NULL,
    rows_loaded INTEGER NOT NULL,
    quality_status VARCHAR(50) NOT NULL,
    anomalies_count INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Idempotence du suivi : une seule ligne par exécution (dag_id, run_id).
    UNIQUE (dag_id, run_id)
);
