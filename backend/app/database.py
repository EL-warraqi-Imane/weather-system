"""
Database service for PostgreSQL operations
"""

import asyncpg
from datetime import datetime
from typing import List, Dict, Optional
import logging
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseService:
    """Service for database operations"""
    
    _pool = None
    
    @classmethod
    async def initialize(cls, 
                        host: str = "localhost",
                        port: int = 5555,
                        database: str = "weather",
                        user: str = "admin",
                        password: str = "admin123"):
        """Initialize database connection pool"""
        try:
            cls._pool = await asyncpg.create_pool(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            
            # Test connection
            async with cls._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            
            logger.info(f"✅ Connected to PostgreSQL at {host}:{port}/{database}")
            
            # Initialize tables
            await cls._initialize_tables()
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    @classmethod
    async def _initialize_tables(cls):
        """Create tables if they don't exist"""
        try:
            async with cls._pool.acquire() as conn:
                # 1. Table Stations
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS weather_stations (
                        id SERIAL PRIMARY KEY,
                        latitude DECIMAL(10, 6) NOT NULL,
                        longitude DECIMAL(10, 6) NOT NULL
                    )
                """)

                # 2. Table Forecasts (Correction du UNIQUE ici)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS weekly_forecasts (
                        id SERIAL PRIMARY KEY,
                        latitude DECIMAL(10, 6),
                        longitude DECIMAL(10, 6),
                        predicted_event VARCHAR(100), 
                        confidence_score DECIMAL(5, 2), 
                        target_timestamp TIMESTAMP NOT NULL,
                        predicted_temperature DECIMAL(5, 2),
                        predicted_humidity DECIMAL(5, 2),
                        predicted_pressure DECIMAL(7, 2),
                        predicted_wind_speed DECIMAL(5, 2),
                        predicted_wind_gusts DECIMAL(5, 2),
                        predicted_precipitation DECIMAL(5, 2),
                        predicted_snowfall DECIMAL(5, 2),
                        predicted_soil_moisture DECIMAL(4, 3),
                        model_version VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (latitude, longitude, target_timestamp)
                    )
                """)
                
                # 3. Table Historical Data
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS historical_data (
                        id SERIAL PRIMARY KEY,
                        simulation_id VARCHAR(100),
                        sequence_index INTEGER,
                        target_date DATE,
                        latitude DECIMAL(10, 6),
                        longitude DECIMAL(10, 6),
                        temperature DECIMAL(5, 2),
                        humidity DECIMAL(5, 2),
                        pressure DECIMAL(7, 2),
                        wind_speed DECIMAL(5, 2),
                        wind_gusts DECIMAL(5, 2),
                        precipitation DECIMAL(5, 2),
                        snowfall DECIMAL(5, 2),
                        soil_moisture DECIMAL(4, 3),
                        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (latitude, longitude, target_date, sequence_index)
                    )
                """)
                await conn.execute("""
                        CREATE  TABLE IF NOT EXISTS station_daily_summary (
                        id SERIAL PRIMARY KEY,
                        station_id INTEGER REFERENCES weather_stations(id), -- Lien vers ta table station
                        analysis_date DATE NOT NULL,
                        avg_temp DOUBLE PRECISION,
                        max_wind_speed DOUBLE PRECISION,
                        total_precipitation DOUBLE PRECISION,
                        humidity_trend VARCHAR(20), -- Ex: "Rising", "Stable", "Falling"
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(station_id, analysis_date) -- Évite les doublons pour une même journée
                                );
                                   """)
                
                # 4. Création des Index (Correction des noms de colonnes)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_forecasts_timestamp 
                    ON weekly_forecasts(target_timestamp)
                """)
                
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_forecasts_location 
                    ON weekly_forecasts(latitude, longitude)
                """)
                
                logger.info("✅ Database tables and indexes initialized")
                
        except Exception as e:
            logger.error(f"❌ Error initializing tables: {e}")
            raise

    @classmethod
    async def save_prediction(cls, prediction_data: Dict) -> bool:
        """Save a prediction to database with UPSERT logic"""
        try:
            async with cls._pool.acquire() as conn:
                # Note: J'ai changé 'predictions' en 'weekly_forecasts' pour correspondre à l'init
                await conn.execute("""
                    INSERT INTO weekly_forecasts (
                        latitude, longitude, target_timestamp,
                        predicted_temperature, predicted_humidity, predicted_pressure,
                        predicted_wind_speed, predicted_wind_gusts, predicted_precipitation,
                        predicted_snowfall, predicted_soil_moisture, model_version
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (latitude, longitude, target_timestamp) 
                    DO UPDATE SET 
                        predicted_temperature = EXCLUDED.predicted_temperature,
                        predicted_humidity = EXCLUDED.predicted_humidity,
                        created_at = CURRENT_TIMESTAMP
                """,
                    prediction_data.get('latitude'),
                    prediction_data.get('longitude'),
                    prediction_data['target_timestamp'],
                    prediction_data['predicted_temperature'],
                    prediction_data['predicted_humidity'],
                    prediction_data['predicted_pressure'],
                    prediction_data['predicted_wind_speed'],
                    prediction_data['predicted_wind_gusts'],
                    prediction_data['predicted_precipitation'],
                    prediction_data['predicted_snowfall'],
                    prediction_data['predicted_soil_moisture'],
                    prediction_data['model_version']
                )
            return True
        except Exception as e:
            logger.error(f"❌ Error saving prediction: {e}")
            return False
    @classmethod
    async def save_historical_data(cls, historical_data: List[Dict]) -> bool:
        """Save historical data to database"""
        try:
            async with cls._pool.acquire() as conn:
                for data in historical_data:
                    await conn.execute("""
                        INSERT INTO historical_data (
                            simulation_id, sequence_index, target_date,
                            latitude, longitude, temperature, humidity,
                            pressure, wind_speed, wind_gusts, precipitation,
                            snowfall, soil_moisture
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    """,
                        data.get('simulation_id', 'default'),
                        data.get('sequence_index', 0),
                        data.get('target_date', datetime.now().date()),
                        data.get('latitude', 48.8566),
                        data.get('longitude', 2.3522),
                        data.get('temperature', 0),
                        data.get('humidity', 0),
                        data.get('pressure', 1013),
                        data.get('wind_speed', 0),
                        data.get('wind_gusts', 0),
                        data.get('precipitation', 0),
                        data.get('snowfall', 0),
                        data.get('soil_moisture', 0.3)
                    )
            
            logger.info(f"✅ Historical data saved: {len(historical_data)} records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving historical data: {e}")
            return False
    
    @classmethod
    async def get_predictions(cls, limit: int = 10, offset: int = 0) -> List[Dict]:
        """Get recent predictions"""
        try:
            async with cls._pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT 
                        prediction_id, target_date, latitude, longitude,
                        predicted_temperature, predicted_humidity, predicted_pressure,
                        predicted_wind_speed, predicted_wind_gusts, predicted_precipitation,
                        predicted_snowfall, predicted_soil_moisture,
                        confidence_score, model_version, processing_time, created_at
                    FROM predictions
                    ORDER BY created_at DESC
                    LIMIT $1 OFFSET $2
                """, limit, offset)
                
                predictions = []
                for row in rows:
                    predictions.append({
                        'prediction_id': row['prediction_id'],
                        'target_date': row['target_date'].isoformat(),
                        'latitude': float(row['latitude']),
                        'longitude': float(row['longitude']),
                        'predicted_temperature': float(row['predicted_temperature']),
                        'predicted_humidity': float(row['predicted_humidity']),
                        'predicted_pressure': float(row['predicted_pressure']),
                        'predicted_wind_speed': float(row['predicted_wind_speed']),
                        'predicted_wind_gusts': float(row['predicted_wind_gusts']),
                        'predicted_precipitation': float(row['predicted_precipitation']),
                        'predicted_snowfall': float(row['predicted_snowfall']),
                        'predicted_soil_moisture': float(row['predicted_soil_moisture']),
                        'confidence_score': float(row['confidence_score']),
                        'model_version': row['model_version'],
                        'processing_time': float(row['processing_time']),
                        'created_at': row['created_at'].isoformat()
                    })
                
                return predictions
                
        except Exception as e:
            logger.error(f"❌ Error getting predictions: {e}")
            return []
    
    @classmethod
    async def get_prediction_stats(cls, days: int = 7) -> Dict:
        """Get prediction statistics for the last N days"""
        try:
            async with cls._pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_predictions,
                        AVG(predicted_temperature) as avg_temperature,
                        AVG(predicted_humidity) as avg_humidity,
                        AVG(confidence_score) as avg_confidence
                    FROM predictions
                    WHERE created_at >= CURRENT_DATE - INTERVAL '1 day' * $1
                """, days)
                
                return {
                    'total_predictions': row['total_predictions'] if row else 0,
                    'avg_temperature': float(row['avg_temperature']) if row and row['avg_temperature'] else 0,
                    'avg_humidity': float(row['avg_humidity']) if row and row['avg_humidity'] else 0,
                    'avg_confidence': float(row['avg_confidence']) if row and row['avg_confidence'] else 0
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting stats: {e}")
            return {}
    
    @classmethod
    @asynccontextmanager
    async def get_connection(cls):
        """Get a database connection from the pool"""
        if cls._pool is None:
            raise RuntimeError("Database not initialized")
        
        conn = await cls._pool.acquire()
        try:
            yield conn
        finally:
            await cls._pool.release(conn)
    
    @classmethod
    async def is_healthy(cls) -> bool:
        """Check database health"""
        try:
            if cls._pool:
                async with cls._pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                return True
        except Exception:
            pass
        return False
    
    @classmethod
    async def close(cls):
        """Close database connections"""
        if cls._pool:
            await cls._pool.close()
            logger.info("🔴 Database connections closed")
    @classmethod
    async def bulk_insert_stations(cls) -> bool:
        import pandas as pd
        import os
        
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(current_dir, "150pointsconrdinations.csv")
            
            async with cls._pool.acquire() as conn:
                # 1. Vérifier si la table est déjà remplie
                count = await conn.fetchval("SELECT COUNT(*) FROM weather_stations")
                if count > 0:
                    return True
                
                if not os.path.exists(file_path):
                    logger.error(f"❌ Fichier introuvable : {file_path}")
                    return False

                # 2. Lire le CSV avec ses 4 colonnes réelles
                # On donne des noms temporaires aux 4 colonnes pour pouvoir les filtrer
                df = pd.read_csv(
                    file_path, 
                    header=None, 
                    names=['old_id', 'latitude', 'longitude', 'nb_events']
                )
                
                # 3. On ne garde QUE la latitude et la longitude
                df_final = df[['latitude', 'longitude']].copy()
                
                # 4. Nettoyage de sécurité (force les nombres, supprime les lignes vides)
                df_final['latitude'] = pd.to_numeric(df_final['latitude'], errors='coerce')
                df_final['longitude'] = pd.to_numeric(df_final['longitude'], errors='coerce')
                df_final = df_final.dropna()

                # 5. Conversion en liste de tuples pour l'insertion
                stations_data = list(df_final.itertuples(index=False, name=None))

                # 6. Insertion dans PostgreSQL
                await conn.copy_records_to_table(
                    'weather_stations',
                    records=stations_data,
                    columns=['latitude', 'longitude']
                )
                
                logger.info(f"✅ {len(stations_data)} stations insérées (Lat/Lon uniquement).")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'insertion automatique : {e}")
            return False
    @classmethod
    async def get_all_stations(cls) -> List[Dict]:
        """Récupère les stations pour le Producer Kafka"""
        try:
            async with cls._pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM weather_stations")
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"❌ Error fetching stations: {e}")
            return []
    @classmethod
    async def get_7day_forecast(cls, lat: float, lon: float) -> List[Dict]:
        """Récupère les prédictions pour les 7 prochains jours (168h)"""
        try:
            async with cls._pool.acquire() as conn:
                # On récupère les colonnes nécessaires + l'event et la confidence
                # Note: Assure-toi que les colonnes predicted_event et confidence_score existent en DB
                query = """
                SELECT 
                    target_timestamp as timestamp,
                    predicted_event as event,
                    confidence_score as confidence,
                    predicted_temperature as temperature,
                    predicted_humidity as humidity,
                    predicted_pressure as pressure,
                    predicted_wind_speed as wind_speed,
                    predicted_precipitation as precipitation,
                    predicted_wind_gusts as wind_gusts_10m,
                    predicted_precipitation as precipitation,
                    predicted_snowfall as snowfall,
                   predicted_soil_moisture as soil_moisture_0_to_7cm
            
                FROM weekly_forecasts
                WHERE ABS(latitude - $1::numeric) < 0.0001 
                  AND ABS(longitude - $2::numeric) < 0.0001
                  AND target_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                ORDER BY target_timestamp ASC
                ;
            """
                rows = await conn.fetch(query, lat, lon)
                
                # Convertit les records asyncpg en liste de dictionnaires
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Error fetching 7-day forecast: {e}")
            return []
    @classmethod
    async def get_forecast_for_expert(cls, lat: float, lon: float) -> List[Dict]:
     """Récupère les données complètes pour le Système Expert"""
     try:
        async with cls._pool.acquire() as conn:
            query = """
            SELECT 
                target_timestamp as timestamp,
                predicted_event,
                confidence_score,
                predicted_temperature as temperature_2m,
                predicted_humidity as relative_humidity_2m,
                predicted_pressure as surface_pressure,
                predicted_wind_speed as wind_speed_10m,
                predicted_wind_gusts as wind_gusts_10m,
                predicted_precipitation as precipitation,
                predicted_snowfall as snowfall,
                predicted_soil_moisture as soil_moisture_0_to_7cm
            FROM weekly_forecasts
            WHERE ABS(latitude - $1::numeric) < 0.0001 
              AND ABS(longitude - $2::numeric) < 0.0001
              AND target_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            ORDER BY target_timestamp ASC
            ; -- On prend 7 jours (168h) pour avoir la timeline complète
            """
            rows = await conn.fetch(query, lat, lon)
            return [dict(row) for row in rows]
     except Exception as e:
        print(f"Erreur DB: {e}")
        return []
    @classmethod
    async def get_station_stats(cls, station_id: int, days: int = 7):
     try:
        async with cls._pool.acquire() as conn:
            query = """
            SELECT 
                analysis_date as date,
                avg_temp,
                max_wind_speed,
                total_precipitation,
                humidity_trend
            FROM station_daily_summary
            WHERE station_id = $1
            AND analysis_date > CURRENT_DATE - make_interval(days => $2)
            ORDER BY analysis_date ASC;
            """
            rows = await conn.fetch(query, station_id, days)
            return [dict(r) for r in rows]
     except Exception as e:
        print(f"Erreur SQL stats station: {e}")
        return []