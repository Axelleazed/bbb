// Store selected departments
let selectedDepartments = new Map();
let map = null;
let geojsonLayer = null;
let currentProcessId = null;

// French departments data
const frenchDepartments = {
    "75": "Paris",
    "77": "Seine-et-Marne",
    "78": "Yvelines",
    "91": "Essonne",
    "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne",
    "95": "Val-d'Oise",
    "01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes", "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes",
    "09": "Ariège", "10": "Aube", "11": "Aude", "12": "Aveyron",
    "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal", "16": "Charente",
    "17": "Charente-Maritime", "18": "Cher", "19": "Corrèze", "21": "Côte-d'Or",
    "22": "Côtes-d'Armor", "23": "Creuse", "24": "Dordogne", "25": "Doubs",
    "26": "Drôme", "27": "Eure", "28": "Eure-et-Loir", "29": "Finistère",
    "2A": "Corse-du-Sud", "2B": "Haute-Corse", "30": "Gard", "31": "Haute-Garonne",
    "32": "Gers", "33": "Gironde", "34": "Hérault", "35": "Ille-et-Vilaine",
    "36": "Indre", "37": "Indre-et-Loire", "38": "Isère", "39": "Jura",
    "40": "Landes", "41": "Loir-et-Cher", "42": "Loire", "43": "Haute-Loire",
    "44": "Loire-Atlantique", "45": "Loiret", "46": "Lot", "47": "Lot-et-Garonne",
    "48": "Lozère", "49": "Maine-et-Loire", "50": "Manche", "51": "Marne",
    "52": "Haute-Marne", "53": "Mayenne", "54": "Meurthe-et-Moselle", "55": "Meuse",
    "56": "Morbihan", "57": "Moselle", "58": "Nièvre", "59": "Nord",
    "60": "Oise", "61": "Orne", "62": "Pas-de-Calais", "63": "Puy-de-Dôme",
    "64": "Pyrénées-Atlantiques", "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales",
    "67": "Bas-Rhin", "68": "Haut-Rhin", "69": "Rhône", "70": "Haute-Saône",
    "71": "Saône-et-Loire", "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie",
    "76": "Seine-Maritime", "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn",
    "82": "Tarn-et-Garonne", "83": "Var", "84": "Vaucluse", "85": "Vendée",
    "86": "Vienne", "87": "Haute-Vienne", "88": "Vosges", "89": "Yonne",
    "90": "Territoire de Belfort"
};

// Initialize the map
function initMap() {
    map = L.map('map').setView([46.603354, 1.888334], 6);
    
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors'
    }).addTo(map);
    
    loadGeoJSON();
}

// Load GeoJSON data
async function loadGeoJSON() {
    try {
        const response = await fetch('https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements.geojson');
        if (!response.ok) throw new Error('Failed to load GeoJSON');
        
        const geojsonData = await response.json();
        createMapLayers(geojsonData);
        
        // Auto-select predefined departments
        setTimeout(() => {
            selectPredefined();
        }, 500);
        
    } catch (error) {
        console.error('Error loading GeoJSON:', error);
        fallbackMap();
    }
}

// Fallback if GeoJSON fails to load
function fallbackMap() {
    const mapContainer = document.getElementById('map');
    mapContainer.innerHTML = `
        <div class="alert alert-warning" style="margin: 20px;">
            <h4><i class="fas fa-exclamation-triangle me-2"></i>Impossible de charger la carte</h4>
            <p>Les limites des départements n'ont pas pu être chargées. Vous pouvez toujours utiliser le bouton "Île-de-France" pour sélectionner les départements prédéfinis.</p>
        </div>
    `;
    
    // Auto-select predefined even without map
    setTimeout(() => {
        selectPredefined();
    }, 100);
}

// Create map layers from GeoJSON
function createMapLayers(geojsonData) {
    function style(feature) {
        const isSelected = selectedDepartments.has(feature.properties.code);
        return {
            fillColor: isSelected ? '#ff6b6b' : '#3388ff',
            weight: isSelected ? 3 : 1,
            opacity: 1,
            color: isSelected ? '#e74c3c' : 'white',
            dashArray: isSelected ? '' : '3',
            fillOpacity: isSelected ? 0.8 : 0.5
        };
    }
    
    function highlightFeature(e) {
        const layer = e.target;
        layer.setStyle({
            weight: 3,
            color: '#666',
            dashArray: '',
            fillOpacity: 0.9
        });
        layer.bringToFront();
        
        layer.bindTooltip(
            `${layer.feature.properties.nom} (${layer.feature.properties.code})<br>Cliquer pour sélectionner`,
            {direction: 'top'}
        ).openTooltip();
    }
    
    function resetHighlight(e) {
        const layer = e.target;
        const code = layer.feature.properties.code;
        const isSelected = selectedDepartments.has(code);
        
        geojsonLayer.resetStyle(e.target);
        
        if (isSelected) {
            layer.setStyle({
                fillColor: '#ff6b6b',
                weight: 3,
                opacity: 1,
                color: '#e74c3c',
                dashArray: '',
                fillOpacity: 0.8
            });
        }
        layer.closeTooltip();
    }
    
    function onFeatureClick(e) {
        const layer = e.target;
        const code = layer.feature.properties.code;
        const name = layer.feature.properties.nom;
        addDepartment(code, name, layer);
    }
    
    function onEachFeature(feature, layer) {
        layer.on({
            mouseover: highlightFeature,
            mouseout: resetHighlight,
            click: onFeatureClick
        });
    }
    
    geojsonLayer = L.geoJSON(geojsonData, {
        style: style,
        onEachFeature: onEachFeature
    }).addTo(map);
    
    map.fitBounds(geojsonLayer.getBounds());
}

// Add department to selection
function addDepartment(code, name, layer = null) {
    if (selectedDepartments.has(code)) return;
    
    selectedDepartments.set(code, { code, name });
    updateSelectedList();
    
    if (layer) {
        layer.setStyle({
            fillColor: '#ff6b6b',
            weight: 3,
            opacity: 1,
            color: '#e74c3c',
            dashArray: '',
            fillOpacity: 0.8
        });
    } else if (geojsonLayer) {
        geojsonLayer.eachLayer(function(l) {
            if (l.feature.properties.code === code) {
                l.setStyle({
                    fillColor: '#ff6b6b',
                    weight: 3,
                    opacity: 1,
                    color: '#e74c3c',
                    dashArray: '',
                    fillOpacity: 0.8
                });
            }
        });
    }
    
    // Update hidden input for form submission
    document.getElementById('selectedDepartments').value = Array.from(selectedDepartments.keys()).join(',');
}

// Remove department from selection
function removeDepartment(code) {
    selectedDepartments.delete(code);
    updateSelectedList();
    
    if (geojsonLayer) {
        geojsonLayer.eachLayer(function(layer) {
            if (layer.feature.properties.code === code) {
                geojsonLayer.resetStyle(layer);
                layer.setStyle({
                    fillColor: '#3388ff',
                    weight: 1,
                    opacity: 1,
                    color: 'white',
                    dashArray: '3',
                    fillOpacity: 0.5
                });
            }
        });
    }
    
    // Update hidden input
    document.getElementById('selectedDepartments').value = Array.from(selectedDepartments.keys()).join(',');
}

// Clear all selections
function clearAllSelections() {
    selectedDepartments.clear();
    updateSelectedList();
    
    if (geojsonLayer) {
        geojsonLayer.eachLayer(function(layer) {
            geojsonLayer.resetStyle(layer);
            layer.setStyle({
                fillColor: '#3388ff',
                weight: 1,
                opacity: 1,
                color: 'white',
                dashArray: '3',
                fillOpacity: 0.5
            });
        });
    }
    
    document.getElementById('selectedDepartments').value = '';
    showNotification('Tous les départements ont été désélectionnés!');
}

// Select predefined Île-de-France departments
function selectPredefined() {
    const predefinedDepartments = ['75', '77', '78', '91', '92', '93', '94', '95'];
    
    predefinedDepartments.forEach(code => {
        if (frenchDepartments[code]) {
            const name = frenchDepartments[code];
            addDepartment(code, name);
        }
    });
    
    showNotification('Départements d\'Île-de-France ajoutés!', 'success');
}

// Show notification
function showNotification(message, type = 'info') {
    // Remove existing notifications
    const existingNotifications = document.querySelectorAll('.notification');
    existingNotifications.forEach(notif => notif.remove());
    
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.innerHTML = `
        <span>${message}</span>
        <button onclick="this.parentElement.remove()" style="background:none;border:none;color:white;cursor:pointer;margin-left:10px">×</button>
    `;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (notification.parentElement) {
            notification.remove();
        }
    }, 3000);
}

// Update the selected departments list display
function updateSelectedList() {
    const container = document.getElementById('selectedList');
    const countElement = document.getElementById('count');
    
    countElement.textContent = selectedDepartments.size;
    container.innerHTML = '';
    
    if (selectedDepartments.size === 0) {
        container.innerHTML = '<div class="empty-message">Aucun département sélectionné.<br>Cliquez sur la carte ou utilisez le bouton "Île-de-France".</div>';
        return;
    }
    
    const sortedDepartments = Array.from(selectedDepartments.values())
        .sort((a, b) => {
            const numA = parseInt(a.code) || a.code;
            const numB = parseInt(b.code) || b.code;
            return numA - numB;
        });
    
    sortedDepartments.forEach(dept => {
        const item = document.createElement('div');
        item.className = 'selected-item';
        item.innerHTML = `
            <div>
                <div class="code">${dept.code}</div>
                <div class="name">${dept.name}</div>
            </div>
            <button class="remove-btn" onclick="removeDepartment('${dept.code}')">×</button>
        `;
        container.appendChild(item);
    });
}

// Form submission handler
document.getElementById('processForm').addEventListener('submit', async function(e) {
    e.preventDefault();
    
    // Get selected departments
    const selectedDepts = document.getElementById('selectedDepartments').value;
    if (!selectedDepts) {
        showNotification('Veuillez sélectionner au moins un département!', 'info');
        return;
    }
    
    // Get other form data
    const targetDate = document.getElementById('targetDate').value;
    const selectedKeywords = Array.from(document.querySelectorAll('input[name="selected_keywords"]:checked'))
        .map(cb => cb.value);
    const customKeywords = document.getElementById('customKeywords').value.trim();
    
    // Validate keywords
    if (selectedKeywords.length === 0 && !customKeywords) {
        showNotification('Veuillez sélectionner au moins un mot-clé!', 'info');
        return;
    }
    
    // Show loading modal
    const loadingModal = new bootstrap.Modal(document.getElementById('loadingModal'));
    loadingModal.show();
    
    // Update status
    document.getElementById('processStatus').innerHTML = `
        <div class="alert alert-info">
            <i class="fas fa-spinner fa-spin me-2"></i>
            Démarrage du traitement pour ${selectedDepts.split(',').length} départements...
        </div>
    `;
    
    try {
        // Prepare form data
        const formData = new FormData();
        formData.append('target_date', targetDate);
        formData.append('selected_departments', selectedDepts);
        selectedKeywords.forEach(keyword => {
            formData.append('selected_keywords', keyword);
        });
        formData.append('custom_keywords', customKeywords);
        
        // Send request
        const response = await fetch('/process', {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Erreur lors du démarrage du traitement');
        }
        
        const result = await response.json();
        currentProcessId = result.process_id;
        
        // Start progress tracking
        startProgressTracking(currentProcessId);
        
    } catch (error) {
        console.error('Error starting process:', error);
        loadingModal.hide();
        showNotification('Erreur: ' + error.message, 'error');
        document.getElementById('processStatus').innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-triangle me-2"></i>
                ${error.message}
            </div>
        `;
    }
});

// Start progress tracking
function startProgressTracking(processId) {
    // Show progress container
    document.getElementById('progressContainer').style.display = 'block';
    
    const progressInterval = setInterval(async () => {
        try {
            const response = await fetch(`/progress/${processId}`);
            if (!response.ok) {
                throw new Error('Erreur de suivi du progrès');
            }
            
            const progress = await response.json();
            updateProgressDisplay(progress);
            
            if (progress.status === 'completed') {
                clearInterval(progressInterval);
                hideLoadingModal();
                showResults(progress);
                showNotification('Traitement terminé avec succès!', 'success');
            } else if (progress.status === 'error') {
                clearInterval(progressInterval);
                hideLoadingModal();
                showNotification('Erreur: ' + (progress.error || 'Erreur inconnue'), 'error');
            }
        } catch (error) {
            console.error('Error checking progress:', error);
        }
    }, 2000);
}

// Update progress display
function updateProgressDisplay(progress) {
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const currentStep = document.getElementById('currentStep');
    
    const stepMessages = {
        'starting': 'Démarrage du traitement...',
        'data_extraction': 'Extraction des données depuis l\'API BOAMP...',
        'keyword_filtering': 'Filtrage par mots-clés...',
        'deduplication': 'Suppression des doublons...',
        'department_filtering': 'Filtrage par départements sélectionnés...',
        'pdf_processing': 'Traitement des PDFs...',
        'processing': 'Traitement en cours...',
        'completed': 'Traitement terminé!'
    };
    
    let message = stepMessages[progress.current_step] || 'Traitement en cours...';
    currentStep.textContent = message;
    
    if (progress.total_records > 0 && progress.processed_records >= 0) {
        const percent = Math.round((progress.processed_records / progress.total_records) * 100);
        progressBar.style.width = percent + '%';
        progressText.textContent = `${percent}% (${progress.processed_records}/${progress.total_records})`;
    }
}

// Hide loading modal
function hideLoadingModal() {
    const modal = bootstrap.Modal.getInstance(document.getElementById('loadingModal'));
    if (modal) {
        modal.hide();
    }
}

// Show results
function showResults(progress) {
    document.getElementById('resultsSection').style.display = 'block';
    
    // Update statistics
    const summaryData = progress.summary_table || [];
    const stats = {
        total: summaryData.length,
        lotsFound: summaryData.filter(row => row.Lots && row.Lots.trim() !== '').length,
        visiteObligatoire: summaryData.filter(row => row['Visite Obligatoire'] === 'yes').length
    };
    
    document.getElementById('summaryStats').innerHTML = `
        <div class="col-md-4">
            <div class="card text-center">
                <div class="card-body">
                    <h3 class="text-primary">${stats.total}</h3>
                    <p class="mb-0">Enregistrements Totaux</p>
                </div>
            </div>
        </div>
        <div class="col-md-4">
            <div class="card text-center">
                <div class="card-body">
                    <h3 class="text-success">${stats.lotsFound}</h3>
                    <p class="mb-0">Lots Trouvés</p>
                </div>
            </div>
        </div>
        <div class="col-md-4">
            <div class="card text-center">
                <div class="card-body">
                    <h3 class="text-warning">${stats.visiteObligatoire}</h3>
                    <p class="mb-0">Visite Obligatoire</p>
                </div>
            </div>
        </div>
    `;
    
    // Update results table
    updateResultsTable(progress);
    
    // Update process status
    document.getElementById('processStatus').innerHTML = `
        <div class="alert alert-success">
            <i class="fas fa-check-circle me-2"></i>
            Traitement terminé! ${stats.total} enregistrements trouvés pour ${progress.departments?.length || 0} départements.
        </div>
    `;
    
    // Set up download buttons
    document.getElementById('downloadFullBtn').onclick = () => {
        if (currentProcessId) {
            window.location.href = `/download/${currentProcessId}`;
        }
    };
    
    document.getElementById('downloadSummaryBtn').onclick = () => {
        if (currentProcessId) {
            window.location.href = `/download-summary/${currentProcessId}`;
        }
    };
}

// Update results table
function updateResultsTable(progress) {
    const summaryData = progress.summary_table || [];
    const tableBody = document.getElementById('resultsTableBody');
    
    if (summaryData.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="8" class="text-center text-muted">Aucun résultat trouvé</td></tr>';
        return;
    }
    
    tableBody.innerHTML = summaryData.map(row => `
        <tr>
            <td>${escapeHtml(row.Keywords || '')}</td>
            <td>${escapeHtml(row.Acheteur || '')}</td>
            <td>${escapeHtml(row.Objet || '')}</td>
            <td>${escapeHtml(row.Lots || '')}</td>
            <td>
                <span class="badge ${row['Visite Obligatoire'] === 'yes' ? 'bg-warning' : 'bg-secondary'}">
                    ${row['Visite Obligatoire'] || 'no'}
                </span>
            </td>
            <td>${escapeHtml(row.Département || '')}</td>
            <td>${escapeHtml(row['Date Limite'] || '')}</td>
            <td>
                ${row['PDF Link'] && row['PDF Link'] !== 'N/A' ? 
                    `<a href="${row['PDF Link']}" target="_blank" class="btn btn-sm btn-outline-primary">📄 PDF</a>` : 
                    '<span class="text-muted">N/A</span>'}
            </td>
        </tr>
    `).join('');
}

// Utility function to escape HTML
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    return text.toString()
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// Initialize everything when page loads
window.onload = function() {
    initMap();
};