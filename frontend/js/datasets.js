// =====================================================
// DATASETS TAB FUNCTIONS
// =====================================================

// REMOVED: const declarations that conflict with ui.js
// Using window.API_BASE and window.currentViewDatasetId from ui.js

async function loadDatasets() {
    console.log('loadDatasets called');
    showStatus('datasetsStatus', '<span class="loading"></span> Loading datasets...', 'info');
    
    try {
        console.log('Fetching from:', `${window.API_BASE}/api/datasets`);
        const res = await fetch(`${window.API_BASE}/api/datasets`);
        
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }
        
        const data = await res.json();
        console.log('Datasets response:', data);
        
        if (!data.success) throw new Error(data.detail || 'Failed to load datasets');
        
        const container = document.getElementById('datasetsList');
        if (!container) {
            console.error('datasetsList element not found!');
            return;
        }
        
        if (!data.datasets || data.datasets.length === 0) {
            container.innerHTML = '<p style="color:#999">No datasets saved yet.</p>';
            showStatus('datasetsStatus', '');
            return;
        }
        
        const html = data.datasets.map(ds => `
            <div class="dataset-list-item" onclick="viewDataset(${ds.id})">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h3 style="margin: 0 0 5px 0; color: #667eea;">${ds.name}</h3>
                        <div class="dataset-meta">
                            ${ds.total_companies} companies • SIC: ${ds.sic_codes || 'N/A'}
                            ${ds.description ? `<br>${ds.description}` : ''}
                            <br><small>Created: ${new Date(ds.created_at).toLocaleDateString()}</small>
                        </div>
                    </div>
                    <div class="dataset-actions">
                        <button class="btn-small btn-primary" onclick="event.stopPropagation(); viewDataset(${ds.id})">View</button>
                    </div>
                </div>
            </div>
        `).join('');
        
        container.innerHTML = html;
        showStatus('datasetsStatus', '');
        console.log('Datasets loaded successfully');
        
    } catch (err) {
        console.error('loadDatasets error:', err);
        showStatus('datasetsStatus', err.message, 'error');
    }
}

async function viewDataset(datasetId) {
    console.log('viewDataset called for:', datasetId);
    showStatus('datasetDetailStatus', '<span class="loading"></span> Loading dataset...', 'info');
    
    try {
        // Get dataset details
        const res = await fetch(`${window.API_BASE}/api/datasets/${datasetId}`);
        const data = await res.json();
        
        if (!data.success) throw new Error(data.detail || 'Failed to load dataset');
        
        window.currentViewDatasetId = datasetId;
        
        // Hide list, show detail
        document.querySelector("#content-datasets > .card").style.display = "none";
        document.getElementById("datasetDetailCard").style.display = "block";
        
        // Set dataset name
        const dataset = data.dataset;
        document.getElementById("datasetDetailName").textContent = dataset.name;
        
        // Set dataset info
        document.getElementById("datasetDetailInfo").innerHTML = `
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">${dataset.total_companies.toLocaleString()}</div>
                    <div>Total Companies</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${dataset.sic_codes || 'N/A'}</div>
                    <div>SIC Codes</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${dataset.counties || 'All'}</div>
                    <div>Counties</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${new Date(dataset.created_at).toLocaleDateString()}</div>
                    <div>Created</div>
                </div>
            </div>
            ${dataset.description ? `<p><strong>Description:</strong> ${dataset.description}</p>` : ''}
            ${dataset.source_file ? `<p><small><strong>Source:</strong> ${dataset.source_file}</small></p>` : ''}
        `;
        
        // Load companies
        console.log('Loading companies for dataset:', datasetId);
        const companiesRes = await fetch(`${window.API_BASE}/api/datasets/${datasetId}/companies?limit=10000`);
        const companiesData = await companiesRes.json();
        
        if (companiesData.success && companiesData.companies) {
            console.log(`Found ${companiesData.companies.length} companies`);
            // renderCompaniesTable is in ui.js
            if (typeof renderCompaniesTable === 'function') {
                renderCompaniesTable('datasetCompanies', companiesData.companies, datasetId);
            } else {
                console.error('renderCompaniesTable function not found!');
                document.getElementById('datasetCompanies').innerHTML = 
                    '<p style="color:#999">Error: Table rendering function not available</p>';
            }
        } else {
            document.getElementById('datasetCompanies').innerHTML = 
                '<p style="color:#999">No companies found or failed to load.</p>';
        }
        
        showStatus('datasetDetailStatus', '');
        
    } catch (err) {
        console.error('viewDataset error:', err);
        showStatus('datasetDetailStatus', err.message, 'error');
    }
}

async function analyzeCurrentDataset() {
    if (!window.currentViewDatasetId) {
        alert('No dataset selected');
        return;
    }
    
    showStatus('datasetDetailStatus', '<span class="loading"></span> Analyzing...', 'info');
    
    try {
        const res = await fetch(`${window.API_BASE}/api/datasets/${window.currentViewDatasetId}/analyze`, {
            method: 'POST'
        });
        const data = await res.json();
        
        if (!data.success) throw new Error(data.detail || 'Analysis failed');
        
        // Show analysis in a modal or alert
        alert(`Analysis complete!\nTotal Companies: ${data.analysis.summary.total_companies}\nData Quality Score: ${data.analysis.data_quality_score}`);
        
        showStatus('datasetDetailStatus', '✅ Analysis complete', 'success');
        
    } catch (err) {
        console.error('analyzeCurrentDataset error:', err);
        showStatus('datasetDetailStatus', err.message, 'error');
    }
}

async function exportCurrentDataset(format) {
    if (!window.currentViewDatasetId) {
        alert('No dataset selected');
        return;
    }
    
    try {
        // Show loading
        showStatus('datasetDetailStatus', `<span class="loading"></span> Exporting as ${format.toUpperCase()}...`, 'info');
        
        // Trigger download
        window.location.href = `${window.API_BASE}/api/datasets/${window.currentViewDatasetId}/export?format=${format}`;
        
        // Clear status after a delay (download should have started)
        setTimeout(() => {
            showStatus('datasetDetailStatus', `✅ Export started`, 'success');
            setTimeout(() => showStatus('datasetDetailStatus', ''), 2000);
        }, 1000);
        
    } catch (err) {
        console.error('exportCurrentDataset error:', err);
        showStatus('datasetDetailStatus', err.message, 'error');
    }
}

async function deleteCurrentDataset() {
    if (!window.currentViewDatasetId) {
        alert('No dataset selected');
        return;
    }
    
    if (!confirm('Are you sure you want to delete this dataset? This action cannot be undone.')) {
        return;
    }
    
    showStatus('datasetDetailStatus', '<span class="loading"></span> Deleting...', 'info');
    
    try {
        const res = await fetch(`${window.API_BASE}/api/datasets/${window.currentViewDatasetId}`, {
            method: 'DELETE'
        });
        
        const data = await res.json();
        
        if (!data.success) throw new Error(data.detail || 'Delete failed');
        
        // Go back to list and refresh
        backToDatasetList();
        loadDatasets();
        
    } catch (err) {
        console.error('deleteCurrentDataset error:', err);
        showStatus('datasetDetailStatus', err.message, 'error');
    }
}

// Make functions globally available
window.loadDatasets = loadDatasets;
window.viewDataset = viewDataset;
window.analyzeCurrentDataset = analyzeCurrentDataset;
window.exportCurrentDataset = exportCurrentDataset;
window.deleteCurrentDataset = deleteCurrentDataset;