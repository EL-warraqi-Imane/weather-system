from pathlib import Path
from . import kb_engine

# 1. Chargement unique au démarrage
BASE_DIR = Path(__file__).resolve().parent 
TTL_PATH = BASE_DIR / "kb_climate_WITH_PROXIES.ttl"
g = kb_engine.load_kb(str(TTL_PATH))

async def get_enriched_analysis(lat: float, lon: float,database):
    # 2. Utilise la fonction SQL que je t'ai donnée avec les ALIAS (temperature_2m, etc.)
    rows = await database.get_forecast_for_expert(lat, lon)
    
    if not rows:
        return []

    final_results = []
    
    # 3. Boucle sur les prédictions
    for i, row in enumerate(rows):
        # IMPORTANT : On donne l'événement prédit par Spark
        top5_candidate = [{
            "event": row["predicted_event"], 
            "probability": float(row["confidence_score"]),
            "threshold": 0.5
        }]
        
        # IMPORTANT : Pour les règles de cumul (ex: pluie 6h), 
        # on passe les lignes précédentes comme historique
        history = rows[:i+1] 
        
        # L'intelligence s'exécute
        analysis = kb_engine.evaluate_top5_with_kb(
            g=g,
            top5=top5_candidate,
            row_features=row,   # Les variables météo
            history_rows=history # L'historique pour les calculs de fenêtre
        )
        
        # 4. Construction de la réponse pour le Frontend
        final_results.append({
            "time": row["timestamp"], # Le nom de la colonne dans ta requête SQL
            "event": analysis["final_event"],
            "status": analysis["kb_status"],
            "explanation": analysis["kb_why"],
            "recommendations": analysis["recommendations"],
            "raw_probability": row["confidence_score"]
        })
        
    return final_results