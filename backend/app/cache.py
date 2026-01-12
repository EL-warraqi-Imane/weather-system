"""
Redis cache service for weather predictions
"""

import redis.asyncio as redis
import json
import pickle
from datetime import datetime, timedelta
from typing import Any, Optional, Union, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisCache:
    """Redis cache service for weather data"""
    
    _client = None
    _prefix = "weather:"
    
    # Cache durations (in seconds)
    CACHE_DURATIONS = {
        'prediction': 3600,  # 1 hour
        'weather_data': 300,  # 5 minutes
        'stats': 60,  # 1 minute
        'session': 1800,  # 30 minutes
    }
    
    @classmethod
    async def initialize(cls, 
                        host: str = "localhost",
                        port: int = 6379,
                        db: int = 0,
                        password: Optional[str] = None):  
        """Initialize Redis connection"""
        try:
            cls._client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=False,  # We'll handle encoding/decoding
                socket_keepalive=True,
                retry_on_timeout=True
            )
            
            # Test connection
            await cls._client.ping()
            
            logger.info(f"✅ Connected to Redis at {host}:{port}")
            
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise
    
    @classmethod
    def _encode_value(cls, value: Any) -> bytes:
        """Encode value for Redis storage"""
        if isinstance(value, (str, int, float, bool)):
            return str(value).encode('utf-8')
        elif isinstance(value, (dict, list)):
            return json.dumps(value).encode('utf-8')
        else:
            return pickle.dumps(value)
    
    @classmethod
    def _decode_value(cls, value: bytes, use_json: bool = True) -> Any:
        """Decode value from Redis"""
        if value is None:
            return None
        
        try:
            if use_json:
                return json.loads(value.decode('utf-8'))
            else:
                return pickle.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError):
            try:
                return value.decode('utf-8')
            except:
                return value
        except pickle.UnpicklingError:
            return value
    
    @classmethod
    async def set(cls, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """Set a value in cache with optional expiration"""
        try:
            encoded_key = f"{cls._prefix}{key}"
            encoded_value = cls._encode_value(value)
            
            if expire:
                await cls._client.setex(encoded_key, expire, encoded_value)
            else:
                await cls._client.set(encoded_key, encoded_value)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error setting cache key {key}: {e}")
            return False
    
    @classmethod
    async def get(cls, key: str, use_json: bool = True) -> Optional[Any]:
        """Get a value from cache"""
        try:
            encoded_key = f"{cls._prefix}{key}"
            value = await cls._client.get(encoded_key)
            
            if value is None:
                return None
            
            return cls._decode_value(value, use_json)
            
        except Exception as e:
            logger.error(f"❌ Error getting cache key {key}: {e}")
            return None
    
    @classmethod
    async def delete(cls, key: str) -> bool:
        """Delete a key from cache"""
        try:
            encoded_key = f"{cls._prefix}{key}"
            result = await cls._client.delete(encoded_key)
            return result > 0
            
        except Exception as e:
            logger.error(f"❌ Error deleting cache key {key}: {e}")
            return False
    
    @classmethod
    async def exists(cls, key: str) -> bool:
        """Check if a key exists in cache"""
        try:
            encoded_key = f"{cls._prefix}{key}"
            return await cls._client.exists(encoded_key) > 0
            
        except Exception as e:
            logger.error(f"❌ Error checking cache key {key}: {e}")
            return False
    
    @classmethod
    async def cache_prediction(cls, 
                              target_date: str,
                              latitude: float,
                              longitude: float,
                              prediction_data: Dict[str, Any]) -> bool:
        """Cache a weather prediction"""
        cache_key = f"prediction:{target_date}:{latitude}:{longitude}"
        return await cls.set(
            cache_key,
            prediction_data,
            expire=cls.CACHE_DURATIONS['prediction']
        )
    
    @classmethod
    async def get_cached_prediction(cls,
                                   target_date: str,
                                   latitude: float,
                                   longitude: float) -> Optional[Dict[str, Any]]:
        """Get a cached weather prediction"""
        cache_key = f"prediction:{target_date}:{latitude}:{longitude}"
        return await cls.get(cache_key)
    
    @classmethod
    async def cache_weather_data(cls,
                                location: str,
                                weather_data: Dict[str, Any]) -> bool:
        """Cache weather data for a location"""
        cache_key = f"weather:{location}"
        return await cls.set(
            cache_key,
            weather_data,
            expire=cls.CACHE_DURATIONS['weather_data']
        )
    
    @classmethod
    async def get_cached_weather_data(cls, location: str) -> Optional[Dict[str, Any]]:
        """Get cached weather data for a location"""
        cache_key = f"weather:{location}"
        return await cls.get(cache_key)
    
    @classmethod
    async def cache_stats(cls, stats_type: str, stats_data: Dict[str, Any]) -> bool:
        """Cache statistics"""
        cache_key = f"stats:{stats_type}"
        return await cls.set(
            cache_key,
            stats_data,
            expire=cls.CACHE_DURATIONS['stats']
        )
    
    @classmethod
    async def get_cached_stats(cls, stats_type: str) -> Optional[Dict[str, Any]]:
        """Get cached statistics"""
        cache_key = f"stats:{stats_type}"
        return await cls.get(cache_key)
    
    @classmethod
    async def increment_counter(cls, counter_name: str, amount: int = 1) -> int:
        """Increment a counter"""
        try:
            cache_key = f"counter:{counter_name}"
            return await cls._client.incrby(cache_key, amount)
        except Exception as e:
            logger.error(f"❌ Error incrementing counter {counter_name}: {e}")
            return 0
    
    @classmethod
    async def get_counter(cls, counter_name: str) -> int:
        """Get a counter value"""
        try:
            cache_key = f"counter:{counter_name}"
            value = await cls._client.get(cache_key)
            return int(value) if value else 0
        except Exception as e:
            logger.error(f"❌ Error getting counter {counter_name}: {e}")
            return 0
    
    @classmethod
    async def set_session(cls, session_id: str, session_data: Dict[str, Any]) -> bool:
        """Set session data"""
        cache_key = f"session:{session_id}"
        return await cls.set(
            cache_key,
            session_data,
            expire=cls.CACHE_DURATIONS['session']
        )
    
    @classmethod
    async def get_session(cls, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data"""
        cache_key = f"session:{session_id}"
        return await cls.get(cache_key)
    
    @classmethod
    async def invalidate_pattern(cls, pattern: str) -> int:
        """Invalidate all keys matching a pattern"""
        try:
            full_pattern = f"{cls._prefix}{pattern}"
            keys = await cls._client.keys(full_pattern)
            
            if keys:
                await cls._client.delete(*keys)
                logger.info(f"🗑️  Invalidated {len(keys)} keys matching {pattern}")
                return len(keys)
            
            return 0
            
        except Exception as e:
            logger.error(f"❌ Error invalidating pattern {pattern}: {e}")
            return 0
    
    @classmethod
    async def invalidate_predictions(cls) -> int:
        """Invalidate all prediction caches"""
        return await cls.invalidate_pattern("prediction:*")
    
    @classmethod
    async def invalidate_weather_data(cls) -> int:
        """Invalidate all weather data caches"""
        return await cls.invalidate_pattern("weather:*")
    
    @classmethod
    async def get_info(cls) -> Dict[str, Any]:
        """Get Redis information"""
        try:
            info = await cls._client.info()
            
            # Convert bytes to strings
            redis_info = {}
            for key, value in info.items():
                if isinstance(value, dict):
                    redis_info[key] = {
                        k.decode('utf-8') if isinstance(k, bytes) else k: 
                        v.decode('utf-8') if isinstance(v, bytes) else v
                        for k, v in value.items()
                    }
                elif isinstance(value, bytes):
                    redis_info[key] = value.decode('utf-8')
                else:
                    redis_info[key] = value
            
            # Get cache stats
            keys = await cls._client.keys(f"{cls._prefix}*")
            redis_info['cache_stats'] = {
                'total_keys': len(keys),
                'prefix': cls._prefix
            }
            
            return redis_info
            
        except Exception as e:
            logger.error(f"❌ Error getting Redis info: {e}")
            return {}
    
    @classmethod
    async def is_healthy(cls) -> bool:
        """Check Redis health"""
        try:
            if cls._client:
                await cls._client.ping()
                return True
        except Exception:
            pass
        return False
    
    @classmethod
    async def close(cls):
        """Close Redis connection"""
        if cls._client:
            await cls._client.close()
            logger.info("🔴 Redis connection closed")
