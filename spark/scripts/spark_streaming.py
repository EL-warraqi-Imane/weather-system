#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def main():
    # Initialiser Spark Session
    spark = SparkSession.builder \
        .appName("WeatherPredictionStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark/checkpoints") \
        .getOrCreate()
    
    # Schéma pour les données historiques
    historical_schema = StructType([
        StructField("simulation_id", StringType(), True),
        StructField("target_date", StringType(), True),
        StructField("sequence_index", IntegerType(), True),
        StructField("total_sequences", IntegerType(), True),
        StructField("data", StructType([
            StructField("timestamp", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_gusts", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("snowfall", DoubleType(), True),
            StructField("soil_moisture", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # Lire le stream depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather-historical-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parser les messages JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), historical_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Agréger par simulation_id (regrouper les 24 points)
    windowed_df = parsed_df \
        .groupBy("simulation_id", "target_date", "latitude", "longitude") \
        .agg(
            collect_list("data").alias("historical_sequence"),
            count("*").alias("sequence_count"),
            max("kafka_timestamp").alias("last_received")
        ) \
        .filter(col("sequence_count") == 24)  # Attendre les 24 points
    
    # Fonction pour traiter chaque batch
    def process_batch(batch_df, batch_id):
        if batch_df.count() > 0:
            print(f"📊 Traitement du batch {batch_id} avec {batch_df.count()} séquences")
            
            # Convertir en Pandas pour traitement
            pandas_df = batch_df.toPandas()
            
            for _, row in pandas_df.iterrows():
                print(f"  🔄 Simulation: {row['simulation_id']}")
                print(f"    Target Date: {row['target_date']}")
                print(f"    Location: ({row['latitude']}, {row['longitude']})")
                print(f"    Sequence complète: ✓")
                
                # Ici, vous pourriez appeler le modèle Python
                # Pour l'instant, on simule juste
                simulate_prediction(row)
    
    def simulate_prediction(row):
        """Simule une prédiction (à remplacer par l'appel au modèle réel)"""
        # Calculer des moyennes simples
        temps = [d['temperature'] for d in row['historical_sequence']]
        humidities = [d['humidity'] for d in row['historical_sequence']]
        
        predicted_temp = sum(temps) / len(temps) + 2.0  # +2 degrés
        predicted_humidity = sum(humidities) / len(humidities)
        
        # Sauvegarder en PostgreSQL
        save_to_postgres({
            'simulation_id': row['simulation_id'],
            'target_date': row['target_date'],
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'predicted_temperature': predicted_temp,
            'predicted_humidity': predicted_humidity,
            'sequence_count': row['sequence_count'],
            'processed_at': str(row['last_received'])
        })
    
    def save_to_postgres(prediction):
        """Sauvegarde une prédiction dans PostgreSQL"""
        try:
            # Créer un DataFrame temporaire
            prediction_df = spark.createDataFrame([prediction])
            
            prediction_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/weather") \
                .option("dbtable", "spark_predictions") \
                .option("user", "admin") \
                .option("password", "admin123") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print(f"    ✅ Sauvegardé dans PostgreSQL")
            
        except Exception as e:
            print(f"    ❌ Erreur PostgreSQL: {e}")
    
    # Démarrer le streaming
    query = windowed_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("🚀 Spark Streaming démarré...")
    query.awaitTermination()

if __name__ == "__main__":
    main()