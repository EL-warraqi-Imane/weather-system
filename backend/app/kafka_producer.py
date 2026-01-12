import json
import asyncio
from datetime import datetime
from aiokafka import AIOKafkaProducer
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self):
        self.producer = None
        self.topic_historical = "weather-historical-data"
        self.topic_predictions = "weather-predictions"
    
    async def initialize(self, bootstrap_servers: str = "localhost:9092"):
        """Initialise le producteur Kafka"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None
            )
            
            await self.producer.start()
            logger.info(f"✅ Producteur Kafka connecté à {bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"❌ Erreur de connexion Kafka: {e}")
            raise
    
    async def send_historical_data(
        self,
        historical_data: List[Dict],
        target_date: str,
        latitude: float,
        longitude: float
    ):
        """Envoie les données historiques sur Kafka"""
        try:
            simulation_id = f"sim_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            for i, data_point in enumerate(historical_data):
                message = {
                    "simulation_id": simulation_id,
                    "target_date": target_date,
                    "sequence_index": i,
                    "total_sequences": len(historical_data),
                    "data": data_point,
                    "latitude": latitude,
                    "longitude": longitude,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Utiliser la combinaison simulation_id + index comme clé
                key = f"{simulation_id}_{i}"
                
                await self.producer.send_and_wait(
                    topic=self.topic_historical,
                    value=message,
                    key=key
                )
            
            logger.info(f"✅ {len(historical_data)} messages envoyés à {self.topic_historical}")
            
        except Exception as e:
            logger.error(f"❌ Erreur d'envoi Kafka: {e}")
    
    async def send_prediction(self, prediction: Dict):
        """Envoie une prédiction sur Kafka"""
        try:
            await self.producer.send_and_wait(
                topic=self.topic_predictions,
                value=prediction,
                key=prediction.get('prediction_id', str(datetime.now().timestamp()))
            )
            
            logger.info(f"✅ Prédiction envoyée à {self.topic_predictions}")
            
        except Exception as e:
            logger.error(f"❌ Erreur d'envoi de prédiction: {e}")
    
    async def is_healthy(self) -> bool:
        """Vérifie si Kafka est connecté"""
        try:
            if self.producer:
                metadata = await self.producer.client.bootstrap()
                return metadata is not None
        except:
            pass
        return False
    
    async def close(self):
        """Ferme la connexion Kafka"""
        if self.producer:
            await self.producer.stop()
            logger.info("🔴 Producteur Kafka arrêté")