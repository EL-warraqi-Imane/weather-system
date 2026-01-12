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
                # Create predictions table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS predictions (
                        id SERIAL PRIMARY KEY,
                        prediction_id VARCHAR(100) UNIQUE NOT NULL,
                        target_date DATE NOT NULL,
                        latitude DECIMAL(10, 6),
                        longitude DECIMAL(10, 6),
                        
                        -- Predictions
                        predicted_temperature DECIMAL(5, 2),
                        predicted_humidity DECIMAL(5, 2),
                        predicted_pressure DECIMAL(7, 2),
                        predicted_wind_speed DECIMAL(5, 2),
                        predicted_wind_gusts DECIMAL(5, 2),
                        predicted_precipitation DECIMAL(5, 2),
                        predicted_snowfall DECIMAL(5, 2),
                        predicted_soil_moisture DECIMAL(4, 3),
                        
                        -- Metadata
                        confidence_score DECIMAL(3, 2),
                        model_version VARCHAR(50),
                        processing_time DECIMAL(10, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create historical data table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS historical_data (
                        id SERIAL PRIMARY KEY,
                        simulation_id VARCHAR(100),
                        sequence_index INTEGER,
                        target_date DATE,
                        latitude DECIMAL(10, 6),
                        longitude DECIMAL(10, 6),
                        
                        -- Weather data
                        temperature DECIMAL(5, 2),
                        humidity DECIMAL(5, 2),
                        pressure DECIMAL(7, 2),
                        wind_speed DECIMAL(5, 2),
                        wind_gusts DECIMAL(5, 2),
                        precipitation DECIMAL(5, 2),
                        snowfall DECIMAL(5, 2),
                        soil_moisture DECIMAL(4, 3),
                        
                        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create indexes
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_predictions_date 
                    ON predictions(target_date)
                """)
                
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_predictions_location 
                    ON predictions(latitude, longitude)
                """)
                
                logger.info("✅ Database tables initialized")
                
        except Exception as e:
            logger.error(f"❌ Error initializing tables: {e}")
            raise
    
    @classmethod
    async def save_prediction(cls, prediction_data: Dict) -> bool:
        """Save a prediction to database"""
        try:
            async with cls._pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO predictions (
                        prediction_id, target_date, latitude, longitude,
                        predicted_temperature, predicted_humidity, predicted_pressure,
                        predicted_wind_speed, predicted_wind_gusts, predicted_precipitation,
                        predicted_snowfall, predicted_soil_moisture,
                        confidence_score, model_version, processing_time
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                """,
                    prediction_data['prediction_id'],
                    prediction_data['target_date'],
                    prediction_data.get('latitude', 48.8566),
                    prediction_data.get('longitude', 2.3522),
                    prediction_data['predicted_temperature'],
                    prediction_data['predicted_humidity'],
                    prediction_data['predicted_pressure'],
                    prediction_data['predicted_wind_speed'],
                    prediction_data['predicted_wind_gusts'],
                    prediction_data['predicted_precipitation'],
                    prediction_data['predicted_snowfall'],
                    prediction_data['predicted_soil_moisture'],
                    prediction_data['confidence_score'],
                    prediction_data['model_version'],
                    prediction_data['processing_time']
                )
            
            logger.info(f"✅ Prediction saved: {prediction_data['prediction_id']}")
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