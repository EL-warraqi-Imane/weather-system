from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional

class WeatherDataPoint(BaseModel):
    """Schéma pour un point de données météo"""
    timestamp: datetime
    temperature: float = Field(..., ge=-50, le=60)
    humidity: float = Field(..., ge=0, le=100)
    pressure: float = Field(..., ge=800, le=1100)
    wind_speed: float = Field(..., ge=0, le=200)
    wind_direction: float = Field(..., ge=0, le=360)
    precipitation: float = Field(..., ge=0)
    snowfall: float = Field(..., ge=0)
    soil_moisture: float = Field(..., ge=0, le=1)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    
class PredictionRequest(BaseModel):
    """Schéma pour une requête de prédiction"""
    target_date: str  # Format: "YYYY-MM-DD"
    latitude: float = Field(48.8566, ge=-90, le=90)
    longitude: float = Field(2.3522, ge=-180, le=180)
    simulation_id: Optional[str] = None
    
class HistoricalDataRequest(BaseModel):
    """Schéma pour générer des données historiques"""
    start_date: str  # Format: "YYYY-MM-DD HH:MM:SS"
    end_date: str    # Format: "YYYY-MM-DD HH:MM:SS"
    location: dict
    num_points: int = 24
    
class PredictionResult(BaseModel):
    """Schéma pour un résultat de prédiction"""
    prediction_id: str
    target_date: datetime
    predicted_temperature: float
    predicted_humidity: float
    predicted_pressure: float
    predicted_wind_speed: float
    predicted_wind_gusts: float
    predicted_precipitation: float
    predicted_snowfall: float
    predicted_soil_moisture: float
    confidence_score: float
    model_version: str
    processing_time: float
    created_at: datetime
    
class HealthResponse(BaseModel):
    """Schéma pour la réponse de santé"""
    status: str
    timestamp: datetime
    services: dict
    model_loaded: bool