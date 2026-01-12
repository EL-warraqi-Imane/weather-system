import torch
import pickle
import numpy as np
from datetime import datetime, timedelta
import os
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherModel:
    """Classe pour charger et utiliser le modèle Transformer pré-entraîné"""
    
    def __init__(self):
        self.model = None
        self.scaler_x = None
        self.scaler_y = None
        self.device = None
        self.is_loaded = False
        
        # Configuration basée sur votre modèle
        self.FEATURES = [
            'temperature_2m', 'relative_humidity_2m', 'surface_pressure',
            'wind_speed_10m', 'wind_gusts_10m', 'precipitation',
            'snowfall', 'soil_moisture_0_to_7cm',
            'latitude', 'longitude', 'day_sin', 'day_cos'
        ]
        
        self.TARGETS = [
            'temperature_2m', 'relative_humidity_2m', 'surface_pressure',
            'wind_speed_10m', 'wind_gusts_10m', 'precipitation',
            'snowfall', 'soil_moisture_0_to_7cm'
        ]
        
        self.SEQ_LEN = 24
    
    def load_model(self, model_path: str, scaler_x_path: str, scaler_y_path: str, device: str = "cpu"):
        """Charge le modèle et les scalers pré-entraînés"""
        try:
            logger.info(f"Chargement du modèle depuis {model_path}")
            
            # Vérifier l'existence des fichiers
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Fichier modèle non trouvé: {model_path}")
            
            # Définir le device
            self.device = torch.device(device)
            logger.info(f"Utilisation du device: {self.device}")
            
            # Charger les scalers
            logger.info("Chargement des scalers...")
            with open(scaler_x_path, 'rb') as f:
                self.scaler_x = pickle.load(f)
            with open(scaler_y_path, 'rb') as f:
                self.scaler_y = pickle.load(f)
            
            # Charger le modèle
            logger.info("Chargement des poids du modèle...")
            # checkpoint = torch.load(model_path, map_location=self.device)
            checkpoint = torch.load(model_path, map_location=self.device, weights_only=True)
            # Créer l'architecture du modèle
            from app.model_architecture import WeatherTransformer
            self.model = WeatherTransformer(
                input_dim=len(self.FEATURES),
                d_model=128,
                nhead=8,
                num_layers=4,
                output_dim=len(self.TARGETS)
            ).to(self.device)
            
            # Charger les poids
            # self.model.load_state_dict(checkpoint['model_state_dict'])
            if isinstance(checkpoint, dict) and 'model_state_dict' in checkpoint:
             self.model.load_state_dict(checkpoint['model_state_dict'])
            else:
              self.model.load_state_dict(checkpoint) # <--- C'est cette ligne qui va sauver ton projet
              self.model.eval()
            
            self.is_loaded = True
            logger.info("✅ Modèle chargé avec succès!")
            logger.info(f"Features: {len(self.FEATURES)}, Targets: {len(self.TARGETS)}")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement du modèle: {e}")
            raise
    
    def prepare_input_sequence(self, historical_data: List[Dict], target_date: datetime, 
                              latitude: float, longitude: float) -> np.ndarray:
        """
        Prépare la séquence d'entrée [24, 12] pour le modèle
        
        Args:
            historical_data: Liste de 24 points de données historiques
            target_date: Date cible pour la prédiction
            latitude: Latitude du lieu
            longitude: Longitude du lieu
        """
        sequence = []
        
        for i, data_point in enumerate(historical_data):
            # Calculer le timestamp du point
            point_time = target_date - timedelta(hours=(23 - i))
            
            # Calculer day_sin et day_cos
            day_of_year = point_time.timetuple().tm_yday
            day_sin = np.sin(2 * np.pi * day_of_year / 365.0)
            day_cos = np.cos(2 * np.pi * day_of_year / 365.0)
            
            # Créer le vecteur de features
            features = [
                data_point.get('temperature', 15.0),        # temperature_2m
                data_point.get('humidity', 60.0),           # relative_humidity_2m
                data_point.get('pressure', 1013.0),         # surface_pressure
                data_point.get('wind_speed', 5.0),          # wind_speed_10m
                data_point.get('wind_gusts', 7.0),          # wind_gusts_10m
                data_point.get('precipitation', 0.0),       # precipitation
                data_point.get('snowfall', 0.0),            # snowfall
                data_point.get('soil_moisture', 0.3),       # soil_moisture_0_to_7cm
                latitude,                                   # latitude
                longitude,                                  # longitude
                float(day_sin),                             # day_sin
                float(day_cos)                              # day_cos
            ]
            
            sequence.append(features)
        
        return np.array(sequence, dtype=np.float32)
    
    def predict(self, input_sequence: np.ndarray) -> Dict:
        """
        Effectue une prédiction avec le modèle
        
        Args:
            input_sequence: Array numpy de shape [24, 12]
        
        Returns:
            Dictionnaire avec les prédictions
        """
        if not self.is_loaded:
            raise RuntimeError("Modèle non chargé. Appelez load_model() d'abord.")
        
        try:
            with torch.no_grad():
                # Normaliser l'entrée
                input_norm = self.scaler_x.transform(input_sequence)
                
                # Convertir en tensor (ajouter batch dimension)
                input_tensor = torch.from_numpy(input_norm).unsqueeze(0).to(self.device)
                
                # Prédiction
                pred_norm = self.model(input_tensor)
                pred_norm = pred_norm.cpu().numpy()
                
                # Dénormaliser la prédiction
                prediction = self.scaler_y.inverse_transform(pred_norm)
                
                # Formater les résultats
                results = {}
                for i, target_name in enumerate(self.TARGETS):
                    results[target_name] = float(prediction[0, i])
                
                # Calculer le score de confiance
                results['confidence'] = self._calculate_confidence(input_sequence)
                results['model_version'] = 'transformer-v1.0'
                results['timestamp'] = datetime.now().isoformat()
                
                return results
                
        except Exception as e:
            logger.error(f"Erreur lors de la prédiction: {e}")
            raise
    
    def _calculate_confidence(self, input_sequence: np.ndarray) -> float:
        """Calcule un score de confiance basé sur la séquence d'entrée"""
        # Calculer la variance des features météo (pas de lat/long/day)
        weather_features = input_sequence[:, :8]  # 8 premières features
        variance = np.var(weather_features, axis=0)
        
        # Moins de variance = plus de confiance
        avg_variance = np.mean(variance)
        confidence = 1.0 / (1.0 + avg_variance)
        
        return min(0.99, max(0.5, confidence))