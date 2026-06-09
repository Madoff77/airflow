CREATE TABLE IF NOT EXISTS weather_observations (
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
    UNIQUE (city, observation_time)
);

CREATE TABLE IF NOT EXISTS ingestion_tracking (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    ingestion_date TIMESTAMP NOT NULL,
    cities_count INTEGER NOT NULL,
    rows_count INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
