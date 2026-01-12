// Configuration
const API_URL = 'http://localhost:8000/api';
let currentUser = null;
let authToken = null;

// Initialisation
document.addEventListener('DOMContentLoaded', function() {
    checkSystemStatus();
    loadRecentPredictions();
    
    // Vérifier si l'utilisateur est déjà connecté
    const savedToken = localStorage.getItem('weather_token');
    if (savedToken) {
        authToken = savedToken;
        verifyToken();
    }
});

// ============ FONCTIONS DE CONNEXION API ============

async function makeApiCall(endpoint, method = 'GET', data = null) {
    const headers = {
        'Content-Type': 'application/json',
    };
    
    if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`;
    }
    
    const config = {
        method: method,
        headers: headers,
    };
    
    if (data && (method === 'POST' || method === 'PUT')) {
        config.body = JSON.stringify(data);
    }
    
    try {
        const response = await fetch(`${API_URL}${endpoint}`, config);
        
        if (response.status === 401) {
            // Token invalide ou expiré
            logout();
            throw new Error('Session expirée');
        }
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Erreur API');
        }
        
        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        showNotification(`Erreur: ${error.message}`, 'error');
        throw error;
    }
}

// ============ FONCTIONS D'AUTHENTIFICATION ============

async function register() {
    const email = document.getElementById('register-email').value;
    const password = document.getElementById('register-password').value;
    const name = document.getElementById('register-name').value;
    
    try {
        const response = await makeApiCall('/auth/register', 'POST', {
            email: email,
            password: password,
            full_name: name
        });
        
        showNotification('Inscription réussie ! Connectez-vous.', 'success');
        hideForms();
    } catch (error) {
        // Gestion d'erreur déjà faite dans makeApiCall
    }
}

async function login() {
    const email = document.getElementById('login-email').value;
    const password = document.getElementById('login-password').value;
    
    try {
        const response = await makeApiCall('/auth/login', 'POST', {
            email: email,
            password: password
        });
        
        authToken = response.access_token;
        localStorage.setItem('weather_token', authToken);
        currentUser = response.user;
        
        showNotification('Connexion réussie !', 'success');
        hideForms();
        updateUIAfterLogin();
    } catch (error) {
        // Gestion d'erreur déjà faite dans makeApiCall
    }
}

async function verifyToken() {
    try {
        const response = await makeApiCall('/auth/verify');
        currentUser = response.user;
        updateUIAfterLogin();
    } catch (error) {
        localStorage.removeItem('weather_token');
        authToken = null;
    }
}

function logout() {
    authToken = null;
    currentUser = null;
    localStorage.removeItem('weather_token');
    document.getElementById('auth-section').style.display = 'block';
    document.getElementById('dashboard').style.display = 'none';
    showNotification('Déconnecté', 'info');
}

// ============ FONCTIONS DE STATION ============

async function loadStations() {
    try {
        const stations = await makeApiCall('/stations');
        const select = document.getElementById('station-select');
        const list = document.getElementById('stations-list');
        
        select.innerHTML = '<option value="">Sélectionner une station</option>';
        list.innerHTML = '';
        
        stations.forEach(station => {
            // Ajouter au select
            const option = document.createElement('option');
            option.value = station.id;
            option.textContent = `${station.name} (${station.latitude}, ${station.longitude})`;
            select.appendChild(option);
            
            // Ajouter à la liste
            const stationDiv = document.createElement('div');
            stationDiv.className = 'station-item';
            stationDiv.innerHTML = `
                <strong>${station.name}</strong><br>
                <small>Lat: ${station.latitude}, Lon: ${station.longitude}</small>
            `;
            list.appendChild(stationDiv);
        });
    } catch (error) {
        console.error('Erreur chargement stations:', error);
    }
}

async function addStation() {
    const name = prompt('Nom de la station:');
    if (!name) return;
    
    const lat = parseFloat(prompt('Latitude:') || '48.8566');
    const lon = parseFloat(prompt('Longitude:') || '2.3522');
    
    try {
        await makeApiCall('/stations', 'POST', {
            name: name,
            latitude: lat,
            longitude: lon,
            altitude: 0,
            description: 'Station ajoutée via interface'
        });
        
        showNotification('Station ajoutée !', 'success');
        loadStations();
    } catch (error) {
        console.error('Erreur ajout station:', error);
    }
}

// ============ FONCTIONS DE PRÉDICTION ============

async function launchPrediction() {
    const stationId = document.getElementById('station-select').value;
    const forecastType = document.getElementById('forecast-type').value;
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate() + 1); // Demain
    
    if (!stationId) {
        showNotification('Veuillez sélectionner une station', 'warning');
        return;
    }
    
    try {
        const response = await makeApiCall('/forecasts', 'POST', {
            station_id: stationId,
            forecast_type: forecastType,
            target_date: targetDate.toISOString().split('T')[0],
            parameters: [
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
                "relative_humidity_2m"
            ]
        });
        
        showNotification('Prédiction lancée ! ID: ' + response.job_id, 'success');
        
        // Commencer à suivre le statut
        trackPredictionStatus(response.job_id);
        
    } catch (error) {
        console.error('Erreur lancement prédiction:', error);
    }
}

async function trackPredictionStatus(jobId) {
    const statusDiv = document.getElementById('forecast-status');
    statusDiv.innerHTML = `<div class="loading">Traitement en cours...</div>`;
    
    // Vérifier le statut toutes les 5 secondes
    const interval = setInterval(async () => {
        try {
            const status = await makeApiCall(`/forecasts/${jobId}/status`);
            
            if (status.status === 'completed') {
                clearInterval(interval);
                statusDiv.innerHTML = `<div class="success">✅ Prédiction terminée !</div>`;
                loadPredictionResults(jobId);
            } else if (status.status === 'failed') {
                clearInterval(interval);
                statusDiv.innerHTML = `<div class="error">❌ Échec de la prédiction</div>`;
            } else {
                statusDiv.innerHTML = `
                    <div class="processing">
                        ⏳ Statut: ${status.status}<br>
                        Progression: ${status.progress || '0'}%
                    </div>
                `;
            }
        } catch (error) {
            clearInterval(interval);
            statusDiv.innerHTML = `<div class="error">Erreur de suivi</div>`;
        }
    }, 5000);
}

async function loadPredictionResults(jobId) {
    try {
        const results = await makeApiCall(`/forecasts/${jobId}/results`);
        
        // Afficher les résultats
        const resultsDiv = document.getElementById('forecast-results');
        resultsDiv.innerHTML = `
            <h3>Résultats de la prédiction</h3>
            <pre>${JSON.stringify(results, null, 2)}</pre>
        `;
        
        // Mettre à jour le graphique
        updateChart(results);
        
        // Recharger les prédictions récentes
        loadRecentPredictions();
        
    } catch (error) {
        console.error('Erreur chargement résultats:', error);
    }
}

async function loadRecentPredictions() {
    try {
        const predictions = await makeApiCall('/forecasts?limit=5');
        const container = document.getElementById('recent-forecasts');
        
        if (predictions.length === 0) {
            container.innerHTML = '<p>Aucune prédiction récente</p>';
            return;
        }
        
        container.innerHTML = predictions.map(pred => `
            <div class="prediction-item">
                <strong>${new Date(pred.created_at).toLocaleDateString()}</strong><br>
                <small>Station: ${pred.station_name || pred.station_id}</small><br>
                <small>Statut: <span class="status-${pred.status}">${pred.status}</span></small>
                <button onclick="viewPrediction('${pred.id}')">Voir</button>
            </div>
        `).join('');
        
    } catch (error) {
        console.error('Erreur chargement prédictions:', error);
    }
}

// ============ FONCTIONS DE GRAPHIQUE ============

function updateChart(data) {
    const ctx = document.getElementById('weather-chart').getContext('2d');
    
    // Détruire le graphique existant
    if (window.weatherChart) {
        window.weatherChart.destroy();
    }
    
    // Préparer les données
    const timestamps = data.predictions?.map(p => new Date(p.timestamp).toLocaleTimeString()) || [];
    const temperatures = data.predictions?.map(p => p.temperature_2m) || [];
    const precipitation = data.predictions?.map(p => p.precipitation) || [];
    
    window.weatherChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: timestamps,
            datasets: [
                {
                    label: 'Température (°C)',
                    data: temperatures,
                    borderColor: 'rgb(255, 99, 132)',
                    backgroundColor: 'rgba(255, 99, 132, 0.5)',
                    yAxisID: 'y',
                },
                {
                    label: 'Précipitation (mm)',
                    data: precipitation,
                    borderColor: 'rgb(54, 162, 235)',
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    yAxisID: 'y1',
                }
            ]
        },
        options: {
            responsive: true,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Température (°C)'
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Précipitation (mm)'
                    },
                    grid: {
                        drawOnChartArea: false,
                    },
                }
            }
        }
    });
}

// ============ FONCTIONS UTILITAIRES ============

async function checkSystemStatus() {
    try {
        const response = await fetch('http://localhost:8000/health');
        const data = await response.json();
        
        const statusElement = document.getElementById('system-status');
        if (data.status === 'online') {
            statusElement.innerHTML = '🟢 Système en ligne';
            statusElement.className = 'status-online';
        } else {
            statusElement.innerHTML = '🔴 Système hors ligne';
            statusElement.className = 'status-offline';
        }
    } catch (error) {
        document.getElementById('system-status').innerHTML = '🔴 Impossible de contacter le backend';
        document.getElementById('system-status').className = 'status-error';
    }
    
    // Vérifier Kafka
    try {
        await fetch('http://localhost:8000/api/kafka/status');
        updateSystemStatus('kafka', true);
    } catch (error) {
        updateSystemStatus('kafka', false);
    }
}

function updateSystemStatus(component, isOnline) {
    const element = document.querySelector(`[data-component="${component}"]`);
    if (element) {
        element.textContent = isOnline ? '🟢 Actif' : '🔴 Inactif';
        element.className = isOnline ? 'status-online' : 'status-offline';
    }
}

function showNotification(message, type = 'info') {
    // Créer une notification simple
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    
    document.body.appendChild(notification);
    
    // Supprimer après 3 secondes
    setTimeout(() => {
        notification.remove();
    }, 3000);
}

function updateUIAfterLogin() {
    document.getElementById('auth-section').style.display = 'none';
    document.getElementById('dashboard').style.display = 'block';
    document.getElementById('user-status').textContent = `Connecté: ${currentUser?.email || 'Utilisateur'}`;
    
    // Charger les données
    loadStations();
    loadRecentPredictions();
}

function showLogin() {
    document.getElementById('login-form').style.display = 'block';
    document.getElementById('register-form').style.display = 'none';
}

function showRegister() {
    document.getElementById('register-form').style.display = 'block';
    document.getElementById('login-form').style.display = 'none';
}

function hideForms() {
    document.getElementById('login-form').style.display = 'none';
    document.getElementById('register-form').style.display = 'none';
}