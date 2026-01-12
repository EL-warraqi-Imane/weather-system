# 🌤️ Weather Forecast System

Système de prédiction météo utilisant un modèle Transformer, Kafka, FastAPI et Angular.

## 🏗️ Architecture
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Angular │────▶│ FastAPI │────▶│ Kafka │
│ Frontend │◀────│ Backend │◀────│ Streaming │
└─────────────┘ └─────────────┘ └─────────────┘
│ │
▼ ▼
┌─────────────┐ ┌─────────────┐
│ PostgreSQL │ │ Spark (ML) │
│ Redis │ │ Transformer │



## 🚀 Démarrage Rapide

### Prérequis
- Docker Desktop
- Git
- Python 3.11+ (pour développement)

### Installation

1. **Cloner le dépôt**
```bash
git clone https://github.com/votre-username/weather-forecast-system.git
cd weather-forecast-system
Télécharger les modèles pré-entraînés
Placez ces fichiers dans backend/models/ :

best_transformer_weather.pth

scaler_x_transformer.pkl

scaler_y_transformer.pkl

Démarrer avec Docker

bash
# Windows
lancement.bat
