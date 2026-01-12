from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import uuid
import asyncio
import numpy as np
from app.schemas import *
from app.model_loader import WeatherModel
from app.kafka_producer import KafkaProducer
from app.database import DatabaseService
from app.cache import RedisCache
import math
# Variables globales
weather_model = WeatherModel()
kafka_producer = KafkaProducer()
db_service = DatabaseService()
redis_cache = RedisCache()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestion du cycle de vie de l'application"""
    # Startup
    print("🚀 Démarrage de l'application Weather Prediction...")
    
    # Charger le modèle
    try:
        weather_model.load_model(
            model_path="models/transformer_best.pth",
            scaler_x_path="models/scaler_x_transformer.pkl",
            scaler_y_path="models/scaler_y_transformer.pkl",
            device="cpu"
        )
    except Exception as e:
        print(f"❌ Erreur lors du chargement du modèle: {e}")
    
    # Initialiser les services
    await kafka_producer.initialize()
    await db_service.initialize()
    await redis_cache.initialize()
    
    print("✅ Application démarrée avec succès")
    
    yield
    
    # Shutdown
    print("🔴 Arrêt de l'application...")
    await kafka_producer.close()
    await db_service.close()
    await redis_cache.close()

app = FastAPI(
    title="Weather Prediction API",
    description="API de prédiction météo utilisant un modèle Transformer",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # À restreindre en production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoint racine
@app.get("/")
async def root():
    return {
        "message": "Weather Prediction API",
        "version": "1.0.0",
        "status": "running",
        "model_loaded": weather_model.is_loaded
    }

# Endpoint de santé
@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Vérifie la santé de tous les services"""
    services_status = {
        "kafka": await kafka_producer.is_healthy(),
        "database": await db_service.is_healthy(),
        "redis": await redis_cache.is_healthy(),
        "model": weather_model.is_loaded
    }
    
    all_healthy = all(services_status.values())
    
    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        timestamp=datetime.now(),
        services=services_status,
        model_loaded=weather_model.is_loaded
    )

# Endpoint principal de prédiction
@app.post("/api/predict", response_model=PredictionResult)
async def predict_weather(
    request: PredictionRequest,
    background_tasks: BackgroundTasks
):
    """
    Endpoint principal pour obtenir une prédiction météo
    
    Workflow:
    1. Vérifier le cache Redis
    2. Générer 24h de données historiques
    3. Publier sur Kafka
    4. Traiter avec le modèle Transformer
    5. Sauvegarder en base
    6. Retourner le résultat
    """
    
    try:
        # 1. Valider et parser la date
        try:
            target_date = datetime.strptime(request.target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Format de date invalide. Utilisez YYYY-MM-DD"
            )
        
        # Vérifier que la date est dans le futur
        if target_date.date() <= datetime.now().date():
            raise HTTPException(
                status_code=400,
                detail="La date doit être dans le futur"
            )
        
        # 2. Vérifier le cache
        cache_key = f"prediction:{request.target_date}:{request.latitude}:{request.longitude}"
        cached_result = await redis_cache.get(cache_key)
        
        if cached_result:
            print(f"✅ Résultat trouvé en cache pour {cache_key}")
            return PredictionResult(**cached_result)
        
        # 3. Générer des données historiques simulées
        print(f"🔄 Génération de données historiques pour {request.target_date}...")
        historical_data = generate_simulated_historical_data(
            target_date=target_date,
            latitude=request.latitude,
            longitude=request.longitude
        )
        
        # 4. Publier sur Kafka (en arrière-plan)
        background_tasks.add_task(
            kafka_producer.send_historical_data,
            historical_data,
            request.target_date,
            request.latitude,
            request.longitude
        )
        
        # 5. Préparer la séquence pour le modèle
        input_sequence = weather_model.prepare_input_sequence(
            historical_data=historical_data,
            target_date=target_date,
            latitude=request.latitude,
            longitude=request.longitude
        )
        
        # 6. Faire la prédiction
        print("🧠 Exécution du modèle Transformer...")
        start_time = datetime.now()
        
        raw_prediction = weather_model.predict(input_sequence)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # 7. Créer le résultat structuré
        prediction_id = str(uuid.uuid4())
        
        result = PredictionResult(
            prediction_id=prediction_id,
            target_date=target_date,
            predicted_temperature=raw_prediction['temperature_2m'],
            predicted_humidity=raw_prediction['relative_humidity_2m'],
            predicted_pressure=raw_prediction['surface_pressure'],
            predicted_wind_speed=raw_prediction['wind_speed_10m'],
            predicted_wind_gusts=raw_prediction['wind_gusts_10m'],
            predicted_precipitation=raw_prediction['precipitation'],
            predicted_snowfall=raw_prediction['snowfall'],
            predicted_soil_moisture=raw_prediction['soil_moisture_0_to_7cm'],
            confidence_score=raw_prediction['confidence'],
            model_version=raw_prediction['model_version'],
            processing_time=processing_time,
            created_at=datetime.now()
        )
        
        # 8. Sauvegarder en base de données (en arrière-plan)
        background_tasks.add_task(
            db_service.save_prediction,
            result.dict()
        )
        
        # 9. Mettre en cache
        background_tasks.add_task(
            redis_cache.set,
            cache_key,
            result.dict(),
            expire=3600  # 1 heure
        )
        
        print(f"✅ Prédiction terminée: {prediction_id}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Erreur lors de la prédiction: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur interne: {str(e)}"
        )

# Endpoint pour générer des données historiques
@app.post("/api/generate-historical")
async def generate_historical(data_request: HistoricalDataRequest):
    """Génère des données historiques simulées"""
    historical_data = generate_simulated_historical_data(
        start_date=datetime.strptime(data_request.start_date, "%Y-%m-%d %H:%M:%S"),
        end_date=datetime.strptime(data_request.end_date, "%Y-%m-%d %H:%M:%S"),
        location=data_request.location,
        num_points=data_request.num_points
    )
    
    return {
        "message": f"Généré {len(historical_data)} points de données",
        "data": historical_data
    }

# Endpoint pour l'historique des prédictions
@app.get("/api/predictions")
async def get_predictions(limit: int = 10, offset: int = 0):
    """Récupère l'historique des prédictions"""
    predictions = await db_service.get_predictions(limit, offset)
    return {
        "predictions": predictions,
        "total": len(predictions)
    }

# Fonction utilitaire pour générer des données simulées
def generate_simulated_historical_data(
    target_date: datetime,
    latitude: float,
    longitude: float,
    num_hours: int = 24
) -> List[Dict]:
    """
    Génère des données météo historiques simulées
    Basé sur des patterns saisonniers réalistes
    """
    historical_data = []
    
    for i in range(num_hours):
        # Calculer le timestamp (24h avant la date cible)
        hour_offset = -(num_hours - 1 - i)
        timestamp = target_date + timedelta(hours=hour_offset)
        
        # Patterns saisonniers
        month = timestamp.month
        hour = timestamp.hour
        
        # Température basée sur le mois et l'heure
        base_temp = 10 + 10 * math.sin(2 * math.pi * (month - 3) / 12)
        daily_variation = 8 * math.sin(2 * math.pi * (hour - 14) / 24)
        temperature = base_temp + daily_variation + np.random.normal(0, 1)
        
        # Humidité (inverse de la température)
        humidity = 70 - 0.5 * temperature + np.random.normal(0, 5)
        humidity = max(20, min(100, humidity))
        
        # Pression atmosphérique
        pressure = 1013 + np.random.normal(0, 3)
        
        # Vent
        wind_speed = 5 + 3 * math.sin(2 * math.pi * hour / 12) + np.random.exponential(2)
        wind_gusts = wind_speed * (1.2 + np.random.uniform(0, 0.3))
        
        # Précipitations (plus probables la nuit)
        if 20 <= hour <= 6:  # Nuit
            precipitation = np.random.exponential(0.5)
            snowfall = precipitation if temperature < 0 else 0
        else:
            precipitation = np.random.exponential(0.1)
            snowfall = 0
        
        # Humidité du sol
        soil_moisture = 0.3 + 0.1 * math.sin(2 * math.pi * month / 12)
        
        data_point = {
            "timestamp": timestamp.isoformat(),
            "temperature": float(temperature),
            "humidity": float(humidity),
            "pressure": float(pressure),
            "wind_speed": float(wind_speed),
            "wind_gusts": float(wind_gusts),
            "precipitation": float(precipitation),
            "snowfall": float(snowfall),
            "soil_moisture": float(soil_moisture),
            "latitude": latitude,
            "longitude": longitude
        }
        
        historical_data.append(data_point)
    
    return historical_data