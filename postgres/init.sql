-- Base de données météo
-- CREATE DATABASE IF NOT EXISTS weather;

\c weather;

-- Table principale des prédictions
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    prediction_id UUID UNIQUE NOT NULL,
    target_date DATE NOT NULL,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    
    -- Prédictions
    predicted_temperature DECIMAL(5, 2),
    predicted_humidity DECIMAL(5, 2),
    predicted_pressure DECIMAL(7, 2),
    predicted_wind_speed DECIMAL(5, 2),
    predicted_wind_gusts DECIMAL(5, 2),
    predicted_precipitation DECIMAL(5, 2),
    predicted_snowfall DECIMAL(5, 2),
    predicted_soil_moisture DECIMAL(4, 3),
    
    -- Métadonnées
    confidence_score DECIMAL(3, 2),
    model_version VARCHAR(50),
    processing_time DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table pour les données historiques reçues
CREATE TABLE IF NOT EXISTS historical_data (
    id SERIAL PRIMARY KEY,
    simulation_id VARCHAR(100),
    sequence_index INTEGER,
    target_date DATE,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    
    -- Données météo
    temperature DECIMAL(5, 2),
    humidity DECIMAL(5, 2),
    pressure DECIMAL(7, 2),
    wind_speed DECIMAL(5, 2),
    wind_gusts DECIMAL(5, 2),
    precipitation DECIMAL(5, 2),
    snowfall DECIMAL(5, 2),
    soil_moisture DECIMAL(4, 3),
    
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table pour les prédictions Spark
CREATE TABLE IF NOT EXISTS spark_predictions (
    id SERIAL PRIMARY KEY,
    simulation_id VARCHAR(100),
    target_date DATE,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    predicted_temperature DECIMAL(5, 2),
    predicted_humidity DECIMAL(5, 2),
    sequence_count INTEGER,
    processed_at TIMESTAMP
);

-- Index pour les performances
CREATE INDEX idx_predictions_date ON predictions(target_date);
CREATE INDEX idx_predictions_location ON predictions(latitude, longitude);
CREATE INDEX idx_historical_simulation ON historical_data(simulation_id);
CREATE INDEX idx_historical_date ON historical_data(target_date);

-- Vue pour les statistiques
CREATE VIEW prediction_stats AS
SELECT 
    DATE(created_at) as prediction_date,
    COUNT(*) as prediction_count,
    AVG(predicted_temperature) as avg_temperature,
    AVG(predicted_humidity) as avg_humidity,
    AVG(confidence_score) as avg_confidence
FROM predictions
GROUP BY DATE(created_at)
ORDER BY prediction_date DESC;

-- Fonction pour nettoyer les anciennes données
CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INTEGER DEFAULT 30)
RETURNS VOID AS $$
BEGIN
    DELETE FROM predictions 
    WHERE created_at < CURRENT_DATE - days_to_keep;
    
    DELETE FROM historical_data 
    WHERE received_at < CURRENT_DATE - days_to_keep;
    
    DELETE FROM spark_predictions 
    WHERE processed_at < CURRENT_DATE - days_to_keep;
    
    RAISE NOTICE 'Nettoyage effectué: données de plus de % jours supprimées', days_to_keep;
END;
$$ LANGUAGE plpgsql;