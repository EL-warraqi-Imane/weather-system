import json
import asyncio
import math
import numpy as np
import asyncpg
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer

# Configuration (Vérifie bien tes ports PostgreSQL)
DB_DSN = "postgresql://admin:admin123@localhost:5555/weather"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "weather-raw-data"

# Paramètres de simulation
WAIT_BETWEEN_DAYS = 180  # Attente de 60 secondes après avoir envoyé 24h de données
WAIT_BETWEEN_HOURS = 0.5 # Petit délai pour ne pas saturer Kafka instantanément (0.5s)

def get_temporal_features(specific_dt):
    """Calcule day_sin et day_cos pour un moment précis"""
    day_of_year = specific_dt.timetuple().tm_yday
    day_sin = np.sin(2 * np.pi * day_of_year / 365.0)
    day_cos = np.cos(2 * np.pi * day_of_year / 365.0)
    return float(day_sin), float(day_cos)

def generate_full_weather(lat, lon, id,current_time):
    """Génère les données météo pour une station et une heure précise"""
    d_sin, d_cos = get_temporal_features(current_time)
    
    # Simulation d'une courbe de température basée sur l'heure (plus froid à 4h, plus chaud à 15h)
    temp_base = 15 + 8 * math.sin((current_time.hour - 8) * 2 * math.pi / 24)
    
    return {
        "station_id":id,
        "timestamp": current_time.isoformat(),
        "latitude": float(lat),
        "longitude": float(lon),
        "temperature_2m": round(temp_base + np.random.normal(0, 0.5), 2),
        "relative_humidity_2m": round(max(0, 60 + np.random.normal(0, 5)), 2),
        "surface_pressure": round(1013 + np.random.normal(0, 2), 2),
        "wind_speed_10m": round(abs(5 + np.random.normal(0, 2)), 2),
        "wind_gusts_10m": round(abs(7 + np.random.normal(0, 3)), 2),
        "precipitation": round(max(0, np.random.exponential(0.01)), 2),
        "snowfall": 0.0,
        "soil_moisture_0_to_7cm": round(0.3 + np.random.normal(0, 0.01), 3),
        "day_sin": d_sin,
        "day_cos": d_cos
    }

async def stream_data():
    # 1. Connexion DB pour récupérer les stations
    try:
        conn = await asyncpg.connect(DB_DSN)
        stations = await conn.fetch("SELECT id,latitude, longitude FROM weather_stations")
        await conn.close()
        print(f"✅ {len(stations)} stations récupérées de la base de données.")
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
        start_date = datetime.now()
        
        while True:
            print(f"🚀 Début de l'envoi pour la journée du {start_date.date()}")

            for hour in range(24):
                current_simulated_time = start_date.replace(hour=hour, minute=0, second=0)
                sequence_counter = {}  
                for st in stations:
                    station_key = f"{st['latitude']}_{st['longitude']}"
                    
                    # ✅ Initialiser le compteur si nécessaire
                    if station_key not in sequence_counter:
                        sequence_counter[station_key] = 0
                    
                    payload = generate_full_weather(st['latitude'], st['longitude'],st["id"] ,current_simulated_time)
                    
                    # ✅ Ajouter le sequence_index
                    payload["sequence_index"] = sequence_counter[station_key]
                    sequence_counter[station_key] += 1
                    
                    key = station_key.encode('utf-8')
                    await producer.send(TOPIC_NAME, value=payload, key=key)
                
                print(f"  ➡️ Heure {hour}h: {len(stations)} messages envoyés.")
                await asyncio.sleep(WAIT_BETWEEN_HOURS)

            print(f"😴 Journée terminée. Attente de {WAIT_BETWEEN_DAYS} secondes...")
            start_date += timedelta(days=1)
            await asyncio.sleep(WAIT_BETWEEN_DAYS)
    except Exception as e:
        print(f"❌ Erreur durant le streaming: {e}")
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(stream_data())