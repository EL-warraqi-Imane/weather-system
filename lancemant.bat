@echo off
chcp 65001 > nul
echo.
echo 🌤️  Démarrage du système de prédiction météo...
echo ==============================================
echo.

:: Vérifier si Docker Desktop est en cours d'exécution
echo Vérification de Docker Desktop...
docker version > nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker Desktop n'est pas en cours d'exécution
    echo 📦 Démarrez Docker Desktop depuis le menu Démarrer
    pause
    exit /b 1
)
echo ✅ Docker Desktop est en cours d'exécution
echo.

:: Vérifier WSL 2
echo Vérification de WSL 2...
wsl --list > nul 2>&1
if %errorlevel% neq 0 (
    echo ⚠️  WSL 2 n'est pas configuré
    echo 📦 Exécutez dans PowerShell (Admin) : wsl --install
)
echo ✅ WSL 2 est disponible
echo.

:: Vérifier les modèles pré-entraînés
echo Vérification des modèles...
if not exist "backend\models\best_transformer_weather.pth" (
    echo ❌ Fichier manquant : backend\models\best_transformer_weather.pth
    goto :missing_models
)
if not exist "backend\models\scaler_x_transformer.pkl" (
    echo ❌ Fichier manquant : backend\models\scaler_x_transformer.pkl
    goto :missing_models
)
if not exist "backend\models\scaler_y_transformer.pkl" (
    echo ❌ Fichier manquant : backend\models\scaler_y_transformer.pkl
    goto :missing_models
)
echo ✅ Tous les modèles sont présents
echo.

:: Créer les dossiers de données
echo Création des dossiers de données...
if not exist "postgres_data" mkdir postgres_data
if not exist "kafka_data" mkdir kafka_data
if not exist "spark_checkpoints" mkdir spark_checkpoints
echo ✅ Dossiers créés
echo.

:: Arrêter les services existants
echo Arrêt des services existants...
docker-compose down 2>nul
echo ✅ Services arrêtés
echo.

:: Démarrer les services
echo Démarrage des services...
echo ⏳ Cette opération peut prendre 2-3 minutes...
docker-compose up -d --build

:: Attendre le démarrage
echo.
echo Attente du démarrage des services...
timeout /t 90 /nobreak >nul

:: Vérifier les services
echo.
echo ==============================================
echo 🔍 VÉRIFICATION DES SERVICES
echo ==============================================
echo.

:: Kafka
echo Vérification de Kafka...
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092 >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ Kafka : Opérationnel
) else (
    echo ❌ Kafka : En cours de démarrage...
)

:: PostgreSQL
echo Vérification de PostgreSQL...
docker-compose exec postgres pg_isready -U admin >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ PostgreSQL : Opérationnel
) else (
    echo ❌ PostgreSQL : Non disponible
)

:: Backend API
echo Vérification du Backend...
curl -s http://localhost:8000/api/health >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ Backend API : Opérationnel
) else (
    echo ⏳ Backend API : Patientez encore 30 secondes...
    timeout /t 30 /nobreak >nul
    curl -s http://localhost:8000/api/health >nul 2>&1
    if %errorlevel% equ 0 (
        echo ✅ Backend API : Maintenant opérationnel
    ) else (
        echo ❌ Backend API : Non disponible
    )
)

:: Frontend
echo Vérification du Frontend...
timeout /t 5 /nobreak >nul
curl -s http://localhost:4200 >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ Frontend : Opérationnel
) else (
    echo ⏳ Frontend : En cours de démarrage...
)

:: Afficher les URLs
echo.
echo ==============================================
echo 🌐 ACCÈS AUX SERVICES
echo ==============================================
echo.
echo   Frontend Angular    : http://localhost:4200
echo   Backend API         : http://localhost:8000
echo   Documentation API   : http://localhost:8000/docs
echo   Kafka UI            : http://localhost:8081
echo   Spark UI            : http://localhost:8080
echo.
echo ==============================================
echo 🔧 INFORMATIONS DE CONNEXION
echo ==============================================
echo.
echo   PostgreSQL :
echo     Host     : localhost
echo     Port     : 5432
echo     Database : weather
echo     User     : admin
echo     Password : admin123
echo.
echo   Redis :
echo     Host : localhost
echo     Port : 6379
echo.
echo ==============================================
echo 📋 COMMANDES UTILES
echo ==============================================
echo.
echo   Afficher les logs      : docker-compose logs -f
echo   Arrêter tous           : docker-compose down
echo   Redémarrer             : docker-compose restart
echo   Voir les conteneurs    : docker-compose ps
echo.
echo ==============================================
echo 🎉 PRÊT À UTILISER !
echo ==============================================
echo.
echo Appuyez sur une touche pour ouvrir le Frontend...
pause >nul
start http://localhost:4200
goto :eof

:missing_models
echo.
echo ==============================================
echo 📥 MODÈLES MANQUANTS
echo ==============================================
echo.
echo Téléchargez ces 3 fichiers et placez-les dans backend\models\ :
echo.
echo   1. best_transformer_weather.pth
echo   2. scaler_x_transformer.pkl
echo   3. scaler_y_transformer.pkl
echo.
echo Sans ces fichiers, le système ne peut pas fonctionner.
echo.
pause
exit /b 1