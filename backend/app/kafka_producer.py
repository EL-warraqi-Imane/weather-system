import json
import asyncio
import numpy as np
import asyncpg
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from decimal import Decimal

# Configuration
DB_DSN = "postgresql://admin:admin123@localhost:5555/weather"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "weather-raw-data"

# Paramètres de simulation
WAIT_BETWEEN_DAYS = 180
WAIT_BETWEEN_HOURS = 0.5

# Setup Open-Meteo API client avec cache permanent
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def get_temporal_features(specific_dt):
    """Calcule day_sin et day_cos pour un moment précis"""
    day_of_year = specific_dt.timetuple().tm_yday
    day_sin = np.sin(2 * np.pi * day_of_year / 365.0)
    day_cos = np.cos(2 * np.pi * day_of_year / 365.0)
    return float(day_sin), float(day_cos)

def fetch_historical_weather_batch(stations, target_date):
    """
    Récupère les données météo HISTORIQUES pour toutes les stations pour une journée donnée
    Utilise l'API archive d'Open-Meteo
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # ✅ Convertir Decimal en float pour éviter les erreurs
    latitudes = [float(st['latitude']) for st in stations]
    longitudes = [float(st['longitude']) for st in stations]
    
    date_str = target_date.strftime("%Y-%m-%d")
    
    # AFFICHER LE NOMBRE DE STATIONS DEMANDÉES
    print(f"\n{'='*80}")
    print(f"📡 REQUÊTE API HISTORIQUE")
    print(f"{'='*80}")
    print(f"Date: {date_str}")
    print(f"Nombre de stations DEMANDÉES: {len(stations)}")
    print(f"Première station: ID={stations[0]['id']}, Lat={latitudes[0]:.6f}, Lon={longitudes[0]:.6f}")
    print(f"Dernière station: ID={stations[-1]['id']}, Lat={latitudes[-1]:.6f}, Lon={longitudes[-1]:.6f}")
    
    # Paramètres API - données historiques
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "precipitation",
            "snowfall",
            "soil_moisture_0_to_7cm",
            "wind_speed_10m",
            "wind_gusts_10m"
        ],
        "timezone": "UTC"
    }
    
    try:
        print(f"⏳ Envoi de la requête à l'API...")
        responses = openmeteo.weather_api(url, params=params)
        
        # VÉRIFIER COMBIEN DE RÉPONSES ONT ÉTÉ REÇUES
        print(f"\n{'='*80}")
        print(f"📥 RÉPONSE DE L'API")
        print(f"{'='*80}")
        print(f"Nombre de réponses REÇUES: {len(responses)}")
        
        if len(responses) != len(stations):
            print(f"⚠️  ATTENTION: Demandé {len(stations)} stations, reçu {len(responses)} réponses!")
        else:
            print(f"✅ Bon! Toutes les stations ont été reçues")
        
        # Organiser les données par station
        stations_data = {}
        
        print(f"\n📊 Détail des réponses:")
        for idx, response in enumerate(responses):
            station = stations[idx] if idx < len(stations) else None
            
            if station is None:
                print(f"⚠️  Réponse {idx+1}: Station non trouvée dans la liste d'origine!")
                continue
            
            # ✅ Convertir en float dès le début
            station_lat = float(station['latitude'])
            station_lon = float(station['longitude'])
            station_key = f"{station_lat}_{station_lon}"
            
            # VÉRIFIER LES COORDONNÉES RETOURNÉES
            api_lat = float(response.Latitude())
            api_lon = float(response.Longitude())
            
            # Extraire les données horaires
            hourly = response.Hourly()
            
            # Créer le DataFrame avec les timestamps
            hourly_data = {
                "timestamp": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                )
            }
            
            # Ajouter toutes les variables météo dans le BON ORDRE
            hourly_data["temperature_2m"] = hourly.Variables(0).ValuesAsNumpy()
            hourly_data["relative_humidity_2m"] = hourly.Variables(1).ValuesAsNumpy()
            hourly_data["surface_pressure"] = hourly.Variables(2).ValuesAsNumpy()
            hourly_data["precipitation"] = hourly.Variables(3).ValuesAsNumpy()
            hourly_data["snowfall"] = hourly.Variables(4).ValuesAsNumpy()
            hourly_data["soil_moisture_0_to_7cm"] = hourly.Variables(5).ValuesAsNumpy()
            hourly_data["wind_speed_10m"] = hourly.Variables(6).ValuesAsNumpy()
            hourly_data["wind_gusts_10m"] = hourly.Variables(7).ValuesAsNumpy()
            
            df = pd.DataFrame(data=hourly_data)
            
            # AFFICHER INFO POUR LES 5 PREMIÈRES ET 5 DERNIÈRES STATIONS
            if idx < 5 or idx >= len(responses) - 5:
                coord_match = "✅" if (abs(api_lat - station_lat) < 0.01 and 
                                      abs(api_lon - station_lon) < 0.01) else "⚠️"
                print(f"  [{idx+1:3d}] Station ID={station['id']:3d} | "
                      f"Demandé: ({station_lat:7.3f}, {station_lon:7.3f}) | "
                      f"Reçu: ({api_lat:7.3f}, {api_lon:7.3f}) {coord_match} | "
                      f"{len(df)} heures")
            elif idx == 5:
                print(f"  ... ({len(responses) - 10} stations intermédiaires) ...")
            
            stations_data[station_key] = {
                'station': station,
                'data': df,
                'api_lat': api_lat,
                'api_lon': api_lon
            }
        
        # RÉSUMÉ FINAL
        print(f"\n{'='*80}")
        print(f"📊 RÉSUMÉ")
        print(f"{'='*80}")
        print(f"Stations demandées: {len(stations)}")
        print(f"Réponses reçues: {len(responses)}")
        print(f"Stations traitées: {len(stations_data)}")
        
        # Vérifier les heures
        if stations_data:
            first_station = next(iter(stations_data.values()))
            num_hours = len(first_station['data'])
            print(f"Heures par station: {num_hours}")
            print(f"Total de messages attendus: {len(stations_data)} × {num_hours} = {len(stations_data) * num_hours}")
        
        print(f"{'='*80}\n")
        
        return stations_data
        
    except Exception as e:
        print(f"\n❌ Erreur lors de la récupération des données API: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_weather_message(station, row, sequence_index):
    """Prépare un message météo formaté pour Kafka"""
    d_sin, d_cos = get_temporal_features(row['timestamp'])
    
    # Gérer les valeurs NaN
    def safe_value(val, default=0.0):
        return float(val) if pd.notna(val) else default
    
    return {
        "station_id": station['id'],
        "timestamp": row['timestamp'].isoformat(),
        "latitude": float(station['latitude']),  # ✅ Conversion explicite
        "longitude": float(station['longitude']),  # ✅ Conversion explicite
        "temperature_2m": round(safe_value(row['temperature_2m']), 2),
        "relative_humidity_2m": round(safe_value(row['relative_humidity_2m']), 2),
        "surface_pressure": round(safe_value(row['surface_pressure']), 2),
        "wind_speed_10m": round(safe_value(row['wind_speed_10m']), 2),
        "wind_gusts_10m": round(safe_value(row['wind_gusts_10m']), 2),
        "precipitation": round(safe_value(row['precipitation']), 2),
        "snowfall": round(safe_value(row['snowfall']), 2),
        "soil_moisture_0_to_7cm": round(safe_value(row['soil_moisture_0_to_7cm'], 0.3), 3),
        "day_sin": d_sin,
        "day_cos": d_cos,
        "sequence_index": sequence_index
    }

async def stream_data():
    # 1. Connexion DB pour récupérer les stations
    try:
        conn = await asyncpg.connect(DB_DSN)
        stations = await conn.fetch("SELECT id, latitude, longitude FROM weather_stations")
        await conn.close()
        print(f"✅ {len(stations)} stations récupérées de la base de données.")
        
        # Convertir en liste de dictionnaires
        stations = [dict(st) for st in stations]
        
    except Exception as e:
        print(f"❌ Erreur DB: {e}")
        return

    # 2. Initialisation Kafka
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()

    try:
        # Commencer par HIER (données historiques disponibles)
        start_date = datetime.now() - timedelta(days=1)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while True:
            print(f"\n{'#'*80}")
            print(f"🚀 TRAITEMENT DE LA JOURNÉE: {start_date.date()}")
            print(f"{'#'*80}")
            
            # Récupérer les données météo HISTORIQUES pour toutes les stations
            stations_data = fetch_historical_weather_batch(stations, start_date)
            
            if stations_data is None:
                print("⚠️ Impossible de récupérer les données, passage à la journée suivante...")
                start_date += timedelta(days=1)
                await asyncio.sleep(10)
                continue
            
            # Envoyer les données heure par heure
            sequence_counters = {}
            total_messages = 0
            
            print(f"\n{'='*80}")
            print(f"📤 ENVOI VERS KAFKA")
            print(f"{'='*80}")
            
            for hour in range(24):
                messages_sent = 0
                
                for station_key, station_info in stations_data.items():
                    station = station_info['station']
                    df = station_info['data']
                    
                    # Initialiser le compteur si nécessaire
                    if station_key not in sequence_counters:
                        sequence_counters[station_key] = 0
                    
                    # Vérifier si on a des données pour cette heure
                    if hour < len(df):
                        row = df.iloc[hour]
                        
                        # Préparer et envoyer le message
                        payload = prepare_weather_message(
                            station, row, sequence_counters[station_key]
                        )
                        
                        key = station_key.encode('utf-8')
                        await producer.send(TOPIC_NAME, value=payload, key=key)
                        
                        sequence_counters[station_key] += 1
                        messages_sent += 1
                        total_messages += 1
                
                print(f"  ➡️  Heure {hour:2d}: {messages_sent:3d} messages envoyés")
                await asyncio.sleep(WAIT_BETWEEN_HOURS)
            
            print(f"\n{'='*80}")
            print(f"✅ ENVOI TERMINÉ")
            print(f"{'='*80}")
            print(f"Total messages envoyés: {total_messages}")
            print(f"Stations ayant envoyé: {len(sequence_counters)}")
            print(f"Messages par station: {total_messages // len(sequence_counters) if sequence_counters else 0}")
            print(f"{'='*80}\n")
            now = datetime.now()
            # On prévoit le prochain réveil à DEMAIN à 01:05 du matin 
            # (on ajoute 1h05 pour laisser le temps à l'API de mettre à jour ses archives)
            tomorrow = (now + timedelta(days=1)).replace(hour=1, minute=5, second=0, microsecond=0)
            
            seconds_to_wait = (tomorrow - now).total_seconds()
            print(f"😴 Attente de {seconds_to_wait} secondes...")
            start_date += timedelta(days=1)
            await asyncio.sleep(seconds_to_wait)
            
    except KeyboardInterrupt:
        print("\n⚠️ Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"❌ Erreur durant le streaming: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await producer.stop()
        print("👋 Producer Kafka arrêté proprement")

if __name__ == "__main__":
    asyncio.run(stream_data())