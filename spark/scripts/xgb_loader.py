import xgboost as xgb
import pandas as pd
import numpy as np
import joblib


class ExtremeEventClassifier:
    def __init__(self, model_path: str, scaler_path: str, label_encoder_path: str):
        # Charger le modèle XGBoost
        self.model = xgb.XGBClassifier()
        self.model.load_model(model_path)
        
        # Charger le Scaler et le LabelEncoder (fichiers .pkl ou .joblib)
        self.scaler = joblib.load(scaler_path)
        self.le = joblib.load(label_encoder_path)
        
        # L'ordre exact des colonnes que ton X_train a vu
        self.FEATURE_ORDER = [
            'latitude', 'longitude', 'occurrence', 'temperature_2m', 'apparent_temperature',
            'dew_point_2m', 'relative_humidity_2m', 'precipitation', 'rain', 'snowfall',
            'snow_depth', 'surface_pressure', 'wind_speed_10m', 'wind_gusts_10m',
            'wind_direction_10m', 'cloud_cover', 'weather_code', 'soil_moisture_0_to_7cm',
            'day_sin', 'day_cos', 'wind_chill_factor', 'temp_humidity_index',
            'pressure_anomaly', 'lat_lon_interaction', 'distance_from_equator',
            'is_coastal', 'lat_squared', 'lon_squared', 'lat_abs_lon',
            'lat_lon_ratio', 'snow_to_precip_ratio', 'lat_temp_interaction'
            # Ajoute ici les autres colonnes si ton modèle en attend 47
        ]

    def predict_event(self, gru_results, lat: float, lon: float):
        # 1. Créer le dictionnaire de base avec les sorties du GRU
        data = gru_results.copy()
        data['latitude'] = lat
        data['longitude'] = lon
        data['occurrence'] = 1 # Valeur par défaut
        
        # Compléter les colonnes que le GRU ne prédit pas (valeurs neutres)
        data['apparent_temperature'] = data.get('apparent_temperature', data['temperature_2m'])
        data['dew_point_2m'] = data.get('dew_point_2m', data['temperature_2m'] - 2)
        data['rain'] = data.get('rain', data['precipitation'])
        data['snow_depth'] = 0.0
        data['wind_direction_10m'] = 180.0
        data['cloud_cover'] = 50.0
        data['weather_code'] = 0.0
        
        # 2. Feature Engineering (Ton code exact du Notebook)
        df = pd.DataFrame([data])
        df['wind_chill_factor'] = df['apparent_temperature'] - df['temperature_2m']
        df['temp_humidity_index'] = df['temperature_2m'] * df['relative_humidity_2m'] / 100
        df['pressure_anomaly'] = df['surface_pressure'] - 1013.25
        df['lat_lon_interaction'] = df['latitude'] * df['longitude']
        df['distance_from_equator'] = np.abs(df['latitude'])
        df['is_coastal'] = (np.minimum(np.abs(df['longitude'] + 120), np.abs(df['longitude'] + 75)) < 10).astype(int)
        df['lat_squared'] = df['latitude'] ** 2
        df['lon_squared'] = df['longitude'] ** 2
        df['lat_abs_lon'] = df['latitude'] * np.abs(df['longitude'])
        df['lat_lon_ratio'] = df['latitude'] / (np.abs(df['longitude']) + 0.001)
        df['snow_to_precip_ratio'] = df['snowfall'] / (df['precipitation'] + 0.001)
        df['lat_temp_interaction'] = df['latitude'] * df['temperature_2m']

        # 3. Prediction
        # On s'assure d'avoir uniquement les colonnes du scaler dans le bon ordre
        X_input = df[self.scaler.feature_names_in_]
        X_scaled = self.scaler.transform(X_input)
        
        pred_idx = self.model.predict(X_scaled)[0]
        return self.le.inverse_transform([pred_idx])[0]