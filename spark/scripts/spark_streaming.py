import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")
# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = "172.17.11.96:9092"
DB_URL = "jdbc:postgresql://localhost:5555/weather"
DB_PROPS = {"user": "admin", "password": "admin123", "driver": "org.postgresql.Driver"}

# Initialisation Spark
spark = SparkSession.builder \
    .appName("WeatherSystemStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2,"+ "org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ DISTRIBUER TOUS LES FICHIERS NÉCESSAIRES
import os
from pyspark import SparkFiles
from pathlib import Path

print("\n" + "="*70)
print("📦 DISTRIBUTION DES FICHIERS AUX EXECUTORS")
print("="*70)

# Chemins des fichiers
BASE_DIR = Path(__file__).resolve().parent.parent

files_to_distribute = {
    "model": str(BASE_DIR / "models" / "gru_best.pth"),
    "scaler_x": str(BASE_DIR / "models" / "scaler_x_transformer.pkl"),
    "scaler_y": str(BASE_DIR / "models" / "scaler_y_transformer.pkl"),
    "model_loader": str(BASE_DIR / "scripts" / "model_loader.py"),
    "model_arch": str(BASE_DIR / "scripts" / "model_architecture.py")
}

# Vérifier et distribuer
for name, path in files_to_distribute.items():
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Fichier non trouvé ({name}): {path}")
    print(f"✅ {name}: {path}")
    spark.sparkContext.addFile(path)

print("✅ Tous les fichiers distribués aux executors")
print("="*70 + "\n")

# Singleton pour le modèle
_model_holder = None

def get_model():
    """Charge le modèle sur chaque executor"""
    global _model_holder
    
    if _model_holder is None:
        import sys
        import os
        from pyspark import SparkFiles
        
        try:
            # Ajouter le répertoire Spark au PYTHONPATH
            spark_files_dir = SparkFiles.getRootDirectory()
            if spark_files_dir not in sys.path:
                sys.path.insert(0, spark_files_dir)
            
            # Récupérer les chemins des fichiers
            model_file = SparkFiles.get("gru_best.pth")
            scaler_x_file = SparkFiles.get("scaler_x_transformer.pkl")  # ✅ CORRIGÉ
            scaler_y_file = SparkFiles.get("scaler_y_transformer.pkl")  # ✅ CORRIGÉ
            
            print(f"🔧 Chargement du modèle...")
            print(f"   📁 Répertoire: {spark_files_dir}")
            print(f"   📄 Model: {os.path.exists(model_file)}")
            print(f"   📄 Scaler X: {os.path.exists(scaler_x_file)}")
            print(f"   📄 Scaler Y: {os.path.exists(scaler_y_file)}")
            
            # Importer WeatherModel
            from model_loader import WeatherModel
            
            _model_holder = WeatherModel()
            _model_holder.load_model(
                model_path=model_file,
                scaler_x_path=scaler_x_file,
                scaler_y_path=scaler_y_file,
                device="cpu"
            )
            
            if _model_holder.is_loaded:
                print(f"✅ Modèle chargé avec succès!")
            else:
                print(f"⚠️  Modèle créé mais is_loaded=False")
                
        except Exception as e:
            print(f"❌ ERREUR dans get_model(): {e}")
            import traceback
            traceback.print_exc()
            raise
    
    return _model_holder

def run_7day_forecast(history, lat, lon):
    """Génère des prévisions sur 7 jours"""
    model = get_model()
    current_seq = history.copy()
    forecast_results = []
    
    start_time = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    
    for i in range(24):  # 7 jours
        target_time = start_time + timedelta(hours=i)
        
        input_data = model.prepare_input_sequence(current_seq[-24:], target_time, lat, lon)
        pred = model.predict(input_data)
        
        forecast_results.append({
            "latitude": float(lat),
            "longitude": float(lon),
            "target_timestamp": target_time,
            "predicted_temperature": float(pred['temperature_2m']),
            "predicted_humidity": float(pred['relative_humidity_2m']),
            "predicted_pressure": float(pred['surface_pressure']),
            "predicted_wind_speed": float(pred['wind_speed_10m']),
            "predicted_wind_gusts": float(pred['wind_gusts_10m']),
            "predicted_precipitation": float(pred['precipitation']),
            "predicted_snowfall": float(pred['snowfall']),
            "predicted_soil_moisture": float(pred['soil_moisture_0_to_7cm']),
            "created_at": datetime.now(),
            "model_version": "gru-v1-bigdata"
        })
        
        new_point = {
            'temperature': pred['temperature_2m'],
            'humidity': pred['relative_humidity_2m'],
            'pressure': pred['surface_pressure'],
            'wind_speed': pred['wind_speed_10m'],
            'wind_gusts': pred['wind_gusts_10m'],
            'precipitation': pred['precipitation'],
            'snowfall': pred['snowfall'],
            'soil_moisture': pred['soil_moisture_0_to_7cm']
        }
        current_seq.append(new_point)
    
    return forecast_results

def process_batch(batch_df, batch_id):
    all_historical_records = []
    print(f"\n{'='*70}")
    print(f"📦 BATCH #{batch_id} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    if batch_df.count() == 0:
        print("⚠️  Batch vide\n")
        return
    
    rows = batch_df.collect()
    all_forecasts = []
    all_analyses = [] # <--- LISTE POUR STOCKER LES ANALYSES
    
    for row in rows:
        # --- PRÉPARATION DES DONNÉES ---
        raw_history = row['historical_sequence'] if row['historical_sequence'] else []
        history_as_dicts = [item.asDict() for item in raw_history]
        
        clean_history = [
            {
                'temperature': x['temperature_2m'],
                'humidity': x['relative_humidity_2m'],
                'pressure': x['surface_pressure'],
                'wind_speed': x['wind_speed_10m'],
                'wind_gusts': x['wind_gusts_10m'],
                'precipitation': x['precipitation'],
                'snowfall': x['snowfall'],
                'soil_moisture': x['soil_moisture_0_to_7cm'],
                'timestamp': x['timestamp']
            }
            for x in history_as_dicts if x.get('timestamp') is not None
        ]
        for idx, point in enumerate(clean_history):
            all_historical_records.append({
                "simulation_id": f"sim_{datetime.now().strftime('%Y%m%d')}", # Id unique par jour
                "sequence_index": idx,
                "target_date": point['timestamp'], # Le timestamp devient ta target_date
                "latitude": row['latitude'],
                "longitude": row['longitude'],
                "temperature": point['temperature'],
                "humidity": point['humidity'],
                "pressure": point['pressure'],
                "wind_speed": point['wind_speed'],
                "wind_gusts": point['wind_gusts'],
                "precipitation": point['precipitation'],
                "snowfall": point['snowfall'],
                "soil_moisture": point['soil_moisture']
            })
        
        if len(clean_history) < 24:
            continue
        
        # Tri pour le modèle GRU
        history = sorted(clean_history, key=lambda x: x['timestamp'])[-24:]
        
        # --- PARTIE ANALYSE ---
        temps = [x['temperature'] for x in clean_history]
        all_analyses.append({
            "latitude": row['latitude'], # On utilise lat/lon pour identifier
            "longitude": row['longitude'],
            "analysis_date": datetime.now().date(),
            "avg_temp": __builtins__.sum(temps) / len(temps),
            "max_wind_speed": __builtins__.max([x['wind_speed'] for x in clean_history]),
            "total_precipitation": __builtins__.sum([x['precipitation'] for x in clean_history]),
            "created_at": datetime.now()
        })
        
        # --- PARTIE PRÉDICTION ---
        try:
            station_forecasts = run_7day_forecast(history, row['latitude'], row['longitude'])
            all_forecasts.extend(station_forecasts)
        except Exception as e:
            print(f" ❌ Erreur prédiction Station ({row['latitude']}): {e}")

    # --- INSERTION EN BASE DE DONNÉES ---
    # Requête pour historical_data
    history_query = """
                INSERT INTO historical_data (
                    simulation_id, sequence_index, target_date, latitude, longitude,
                    temperature, humidity, pressure, wind_speed, wind_gusts,
                    precipitation, snowfall, soil_moisture
                ) VALUES (
                    %(simulation_id)s, %(sequence_index)s, %(target_date)s, %(latitude)s, %(longitude)s,
                    %(temperature)s, %(humidity)s, %(pressure)s, %(wind_speed)s, %(wind_gusts)s,
                    %(precipitation)s, %(snowfall)s, %(soil_moisture)s
                )
            """
            
    # --- INSERTION EN BASE DE DONNÉES ---
    if all_historical_records or all_forecasts or all_analyses:
        conn = None
        try:
            conn = psycopg2.connect(
                host="localhost", 
                port="5555", 
                database="weather", 
                user="admin", 
                password="admin123"
            )
            cur = conn.cursor()
            
            # 1. Insertion des données HISTORIQUES
            history_query = """
    INSERT INTO historical_data (
        simulation_id, sequence_index, target_date, latitude, longitude,
        temperature, humidity, pressure, wind_speed, wind_gusts,
        precipitation, snowfall, soil_moisture
    ) VALUES (
        %(simulation_id)s, %(sequence_index)s, %(target_date)s, %(latitude)s, %(longitude)s,
        %(temperature)s, %(humidity)s, %(pressure)s, %(wind_speed)s, %(wind_gusts)s,
        %(precipitation)s, %(snowfall)s, %(soil_moisture)s
    )
     ON CONFLICT (latitude, longitude, target_date, sequence_index) DO NOTHING
            """
            if all_historical_records:
                cur.executemany(history_query, all_historical_records)
                print(f"✅ {len(all_historical_records)} points historiques sauvegardés.")

            # 2. Insertion des PRÉDICTIONS
            forecast_query = """
                INSERT INTO weekly_forecasts (
                    latitude, longitude, target_timestamp, predicted_temperature, 
                    predicted_humidity, predicted_pressure, predicted_wind_speed, 
                    predicted_wind_gusts, predicted_precipitation, predicted_snowfall, 
                    predicted_soil_moisture, created_at, model_version
                ) VALUES (
                    %(latitude)s, %(longitude)s, %(target_timestamp)s, %(predicted_temperature)s,
                    %(predicted_humidity)s, %(predicted_pressure)s, %(predicted_wind_speed)s,
                    %(predicted_wind_gusts)s, %(predicted_precipitation)s, %(predicted_snowfall)s,
                    %(predicted_soil_moisture)s, %(created_at)s, %(model_version)s
                ) ON CONFLICT (latitude, longitude, target_timestamp) DO UPDATE SET 
                predicted_temperature = EXCLUDED.predicted_temperature, created_at = EXCLUDED.created_at;
            """
            if all_forecasts:
                cur.executemany(forecast_query, all_forecasts)

            # 3. Insertion des ANALYSES
            analysis_query = """
                INSERT INTO station_daily_summary (
                    station_id, analysis_date, avg_temp, max_wind_speed, 
                    total_precipitation, created_at
                ) VALUES (
                    (SELECT id FROM weather_stations WHERE latitude=%(latitude)s AND longitude=%(longitude)s LIMIT 1),
                    %(analysis_date)s, %(avg_temp)s, %(max_wind_speed)s, %(total_precipitation)s, %(created_at)s
                ) ON CONFLICT (station_id, analysis_date) DO UPDATE SET avg_temp = EXCLUDED.avg_temp;
            """
            if all_analyses:
                cur.executemany(analysis_query, all_analyses)
            
            conn.commit()
            print(f"✅ Batch #{batch_id} terminé avec succès.")
            
        except Exception as e:
            print(f"❌ ERREUR SQL Critique : {e}")
            if conn: conn.rollback()
        finally:
            if conn: 
                cur.close()
                conn.close()
schema = StructType([
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature_2m", DoubleType()),
    StructField("relative_humidity_2m", DoubleType()),
    StructField("surface_pressure", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("wind_gusts_10m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("snowfall", DoubleType()),
    StructField("soil_moisture_0_to_7cm", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("day_sin", DoubleType()),
    StructField("day_cos", DoubleType())
])

# --- LECTURE KAFKA ---
print("🚀 DÉMARRAGE DU SYSTÈME DE PRÉVISION MÉTÉO")
print("="*70)
print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
print(f"📊 Topic: weather-raw-data")
print(f"🗄️  PostgreSQL: {DB_URL}")
print(f"⏱️  Trigger: 1 minute")
print(f"🪟 Fenêtre: 24h glissantes")
print("="*70 + "\n")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "weather-raw-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp")))

windowed_df = parsed_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        "latitude", 
        "longitude",
        window("event_time", "24 hours")
    ) \
    .agg(
        collect_list(struct(
            "temperature_2m",
            "relative_humidity_2m", 
            "surface_pressure",
            "wind_speed_10m",
            "wind_gusts_10m",
            "precipitation",
            "snowfall",
            "soil_moisture_0_to_7cm",
            "timestamp",
            "event_time"
        )).alias("historical_sequence")
    )\
    .filter(size(col("historical_sequence")) >= 24)

query = windowed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .start()

print("✅ Streaming démarré. En attente de données...\n")

query.awaitTermination()