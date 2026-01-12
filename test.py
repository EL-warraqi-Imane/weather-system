#!/usr/bin/env python3
# test_system.py

import requests
import json
from datetime import datetime, timedelta

def test_prediction():
    """Teste l'endpoint de prédiction"""
    
    # URL du backend
    BASE_URL = "http://localhost:8000"
    
    # Date de demain
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Requête de prédiction
    payload = {
        "target_date": tomorrow,
        "latitude": 48.8566,  # Paris
        "longitude": 2.3522,
        "simulation_id": "test_001"
    }
    
    print(f"🌤️  Test de prédiction pour le {tomorrow} à Paris...")
    
    try:
        # Faire la requête
        response = requests.post(
            f"{BASE_URL}/api/predict",
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print("✅ Prédiction réussie!")
            print(f"   ID: {result['prediction_id']}")
            print(f"   Température: {result['predicted_temperature']:.1f}°C")
            print(f"   Humidité: {result['predicted_humidity']:.1f}%")
            print(f"   Pression: {result['predicted_pressure']:.1f} hPa")
            print(f"   Confiance: {result['confidence_score']:.2%}")
            print(f"   Temps de traitement: {result['processing_time']:.2f}s")
            
            return True
        else:
            print(f"❌ Erreur: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def test_health():
    """Teste l'endpoint de santé"""
    
    BASE_URL = "http://localhost:8000"
    
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=5)
        
        if response.status_code == 200:
            health = response.json()
            print("🏥 État des services:")
            
            for service, status in health['services'].items():
                status_icon = "✅" if status else "❌"
                print(f"   {status_icon} {service}: {'actif' if status else 'inactif'}")
            
            print(f"   📊 Modèle chargé: {'✅' if health['model_loaded'] else '❌'}")
            return True
        else:
            print(f"❌ Erreur santé: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Impossible de joindre le backend: {e}")
        return False

def main():
    print("🧪 Test du système de prédiction météo")
    print("=" * 40)
    
    # Test de santé
    if not test_health():
        print("\n⚠️  Le système n'est pas prêt. Vérifiez que tous les services sont démarrés.")
        return
    
    print("\n" + "=" * 40)
    
    # Test de prédiction
    test_prediction()

if __name__ == "__main__":
    main()