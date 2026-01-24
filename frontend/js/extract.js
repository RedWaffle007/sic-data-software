// =====================================================
// EXTRACT / ANALYZE / ENRICH FLOW
// =====================================================

// REMOVED: const declarations that conflict with ui.js
// Using window.API_BASE and window.currentJobId etc from ui.js

async function extractCompanies() {
    const sicCodes = document.getElementById("sicCodes").value
        .split(",")
        .map(s => s.trim())
        .filter(Boolean);
    
    const countiesRaw = document.getElementById("counties").value.trim();
    const counties = countiesRaw ? countiesRaw.split(",").map(s => s.trim()) : null;
    
    if (!sicCodes.length) {
        showStatus("extractStatus", "Please enter at least one SIC code", "error");
        return;
    }
    
    window.currentSicCodes = sicCodes;
    window.currentCounties = counties;
    
    // RESET enrichment state
    document.getElementById("download-enriched-btn").style.display = "none";
    window.enrichmentJobId = null;
    
    // Hide previous results
    document.getElementById("analysisCard").style.display = "none";
    document.getElementById("enrichmentCard").style.display = "none";
    
    showStatus("extractStatus", "<span class='loading'></span> Extracting...", "info");
    
    try {
        console.log('Extracting with SIC:', sicCodes, 'Counties:', counties);
        const res = await fetch(`${window.API_BASE}/api/extract`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                sic_codes: sicCodes,
                counties: counties
            })
        });
        
        const data = await res.json();
        console.log('Extract response:', data);
        
        if (!data.success) throw new Error(data.detail || "Extraction failed");
        
        window.currentJobId = data.job_id;
        window.currentDatasetFile = data.current_dataset;
        
        showStatus("extractStatus", "✅ Extraction complete", "success");
        displayDatasetInfo(data);
        
    } catch (err) {
        console.error('extractCompanies error:', err);
        showStatus("extractStatus", err.message, "error");
    }
}

function displayDatasetInfo(data) {
    document.getElementById("datasetCard").style.display = "block";
    
    const lastStage = data.stage_results[
        data.stages_completed[data.stages_completed.length - 1]
    ];
    
    document.getElementById("datasetInfo").innerHTML = `
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">${lastStage.total_companies.toLocaleString()}</div>
                <div>Total Companies</div>
            </div>
        </div>
        <p><strong>Dataset:</strong> ${data.current_dataset}</p>
    `;
}

async function analyzeDataset() {
    if (!window.currentDatasetFile) {
        alert("No dataset available. Extract companies first.");
        return;
    }
    
    console.log("Starting analysis for dataset:", window.currentDatasetFile);
    
    // Force show the analysis card
    const analysisCard = document.getElementById("analysisCard");
    analysisCard.style.display = "block";
    
    // Clear previous results and show loading
    document.getElementById("analysisResults").innerHTML = 
        '<div class="status-box info"><span class="loading"></span> Analyzing...</div>';
    
    try {
        const res = await fetch(`${window.API_BASE}/api/analyze`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ dataset_file: window.currentDatasetFile })
        });
        
        const data = await res.json();
        console.log("Analysis response:", data);
        
        if (!data.success) throw new Error(data.detail || "Analysis failed");
        
        if (data.analysis) {
            // Display the analysis results
            const analysisHTML = displayAnalysis(data.analysis);
            
            // Add success message at the top
            const fullHTML = `
                <div class="status-box success" style="margin-bottom: 20px;">
                    ✅ Analysis complete
                </div>
                ${analysisHTML}
            `;
            
            document.getElementById("analysisResults").innerHTML = fullHTML;
            
            // Scroll to analysis section
            analysisCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        } else {
            document.getElementById("analysisResults").innerHTML = 
                '<div class="status-box error">Analysis completed but no data returned.</div>';
        }
        
    } catch (err) {
        console.error("Analysis error:", err);
        document.getElementById("analysisResults").innerHTML = 
            `<div class="status-box error">❌ ${err.message}</div>`;
    }
}

async function enrichDataset() {
    if (!window.currentDatasetFile) {
        alert("No dataset available. Extract companies first.");
        return;
    }
    
    document.getElementById("enrichmentCard").style.display = "block";
    showStatus("enrichmentStatus", "<span class='loading'></span> Enriching your dataset...", "info");
    
    const progress = document.getElementById("enrichmentProgressBar");
    progress.style.display = "block";
    progress.value = 0;
    
    try {
        const res = await fetch(`${window.API_BASE}/api/enrich`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ dataset_file: window.currentDatasetFile, output_format: "parquet" })
        });
        
        const data = await res.json();
        console.log('Enrich response:', data);
        
        if (!data.success) throw new Error(data.detail || "Enrichment failed");
        
        window.enrichmentJobId = data.job_id;
        pollEnrichmentStatus(data.job_id);
        
    } catch (err) {
        console.error('enrichDataset error:', err);
        showStatus("enrichmentStatus", "❌ " + err.message, "error");
    }
}

async function pollEnrichmentStatus(jobId) {
    try {
        const res = await fetch(`${window.API_BASE}/api/status/${jobId}`);
        const data = await res.json();
        
        const progress = document.getElementById("enrichmentProgressBar");
        
        if (data.status === "completed") {
            showStatus("enrichmentStatus", "✅ Enrichment complete", "success");
            progress.value = 100;
            
            document.getElementById("download-enriched-btn").style.display = "inline-block";
            
            const enrichedFile = data.result.output_file;
            window.currentDatasetFile = enrichedFile;
            window.enrichmentJobId = jobId;
            
            // Update dataset info
            document.getElementById("datasetInfo").innerHTML = `
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-value">
                            ${data.result.enrichment_stats.total_processed.toLocaleString()}
                        </div>
                        <div>Total Companies</div>
                    </div>
                </div>
                <p><strong>Dataset:</strong> ${enrichedFile}</p>
                <p style="color: #48bb78;"><strong>Status:</strong> Enriched dataset ready</p>
            `;
            
            const coverage = data.result.enrichment_stats.coverage || {};
            document.getElementById("coverageStats").innerHTML = `
                <h3 style="margin-top:15px;">📈 Data Enrichment Coverage</h3>
                <div class="stats-grid">
                    <div class="stat-card">📞 Phone: ${coverage.phone || "0%"}</div>
                    <div class="stat-card">🌐 Website: ${coverage.website || "0%"}</div>
                    <div class="stat-card">👤 PSC: ${coverage.psc || "0%"}</div>
                    <div class="stat-card">🏢 Officers: ${coverage.officers || "0%"}</div>
                </div>
            `;
            
        } else if (data.status === "failed") {
            showStatus("enrichmentStatus", "❌ " + data.error, "error");
            
        } else {
            const processed = data.processed || 0;
            const total = data.total || 1;
            
            const percent = Math.round((processed / total) * 100);
            progress.value = percent;
            
            showStatus(
                "enrichmentStatus",
                `<span class='loading'></span> Enriching... ${processed} / ${total} companies`,
                "info"
            );
            
            setTimeout(() => pollEnrichmentStatus(jobId), 2000);
        }
    } catch (err) {
        console.error('pollEnrichmentStatus error:', err);
        showStatus("enrichmentStatus", "❌ Error checking status: " + err.message, "error");
    }
}

function downloadRaw() {
    if (!window.currentJobId) {
        alert("No raw dataset available yet.");
        return;
    }
    window.location.href = `${window.API_BASE}/api/download/${window.currentJobId}?format=csv`;
}

function downloadEnriched() {
    if (!window.enrichmentJobId) {
        alert("No enriched dataset available yet.");
        return;
    }
    window.location.href = `${window.API_BASE}/api/download/${window.enrichmentJobId}?format=csv`;
}

async function saveToDatabase() {
    const name = document.getElementById("saveDatasetName").value.trim();
    const description = document.getElementById("saveDatasetDesc").value.trim();
    
    if (!name) {
        showStatus("saveModalStatus", "Please enter a dataset name", "error");
        return;
    }
    
    if (!window.currentDatasetFile) {
        showStatus("saveModalStatus", "No dataset to save", "error");
        return;
    }
    
    showStatus("saveModalStatus", "<span class='loading'></span> Saving...", "info");
    
    try {
        const res = await fetch(`${window.API_BASE}/api/datasets/save`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                dataset_name: name,
                parquet_file: window.currentDatasetFile,
                sic_codes: window.currentSicCodes || [],
                counties: window.currentCounties,
                description: description || null
            })
        });
        
        const data = await res.json();
        console.log('Save response:', data);
        
        if (!data.success) throw new Error(data.detail || "Save failed");
        
        showStatus("saveModalStatus", `✅ ${data.message}`, "success");
        
        setTimeout(() => {
            closeSaveModal();
            switchTab('datasets');
        }, 1500);
        
    } catch (err) {
        console.error('saveToDatabase error:', err);
        showStatus("saveModalStatus", err.message, "error");
    }
}

// Make functions globally available
window.extractCompanies = extractCompanies;
window.analyzeDataset = analyzeDataset;
window.enrichDataset = enrichDataset;
window.downloadRaw = downloadRaw;
window.downloadEnriched = downloadEnriched;
window.saveToDatabase = saveToDatabase;