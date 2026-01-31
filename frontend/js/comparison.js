// =====================================================
// COMPARISON TAB - Compare Initial vs Final Datasets
// =====================================================

// Global state for comparison
window.comparisonInitialData = null;
window.comparisonFinalData = null;
window.comparisonFinalFile = null;

// =====================================================
// REGION FILTER FOR COMPARISON
// =====================================================
function onComparisonRegionChange() {
    const regionSelect = document.getElementById("comparisonRegionFilter");
    const countiesInput = document.getElementById("comparisonCounties");
    
    const selectedOptions = Array.from(regionSelect.selectedOptions);
    const selectedRegions = selectedOptions.map(option => option.value);
    
    if (selectedRegions.length > 0) {
        const allCounties = [];
        selectedRegions.forEach(region => {
            if (window.ENGLAND_REGIONS[region]) {
                allCounties.push(...window.ENGLAND_REGIONS[region]);
            }
        });
        
        const uniqueCounties = [...new Set(allCounties)];
        countiesInput.value = uniqueCounties.join(", ");
    } else {
        countiesInput.value = "";
    }
}

function clearComparisonFilters() {
    document.getElementById("comparisonSicCodes").value = "";
    document.getElementById("comparisonCounties").value = "";
    
    const regionSelect = document.getElementById("comparisonRegionFilter");
    Array.from(regionSelect.options).forEach(option => {
        option.selected = false;
    });
}

// =====================================================
// LOAD INITIAL EXTRACTION
// =====================================================
async function loadInitialExtraction() {
    const sicCodes = document.getElementById("comparisonSicCodes").value
        .split(",")
        .map(s => s.trim())
        .filter(Boolean);
    
    const countiesRaw = document.getElementById("comparisonCounties").value.trim();
    const counties = countiesRaw ? countiesRaw.split(",").map(s => s.trim()) : null;
    
    if (!sicCodes.length) {
        showStatus("comparisonInitialStatus", "Please enter at least one SIC code", "error");
        return;
    }
    
    showStatus("comparisonInitialStatus", "<span class='loading'></span> Extracting and analyzing initial dataset...", "info");
    
    try {
        // Step 1: Extract companies
        console.log('Comparison: Extracting with SIC:', sicCodes, 'Counties:', counties);
        const extractRes = await fetch(`${window.API_BASE}/api/extract`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                sic_codes: sicCodes,
                counties: counties
            })
        });
        
        const extractData = await extractRes.json();
        console.log('Comparison: Extract response:', extractData);
        
        if (!extractData.success) throw new Error(extractData.detail || "Extraction failed");
        
        const datasetFile = extractData.current_dataset;
        
        // Step 2: Analyze the extracted dataset
        const analyzeRes = await fetch(`${window.API_BASE}/api/analyze`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ dataset_file: datasetFile })
        });
        
        const analyzeData = await analyzeRes.json();
        console.log('Comparison: Analysis response:', analyzeData);
        
        if (!analyzeData.success) throw new Error(analyzeData.detail || "Analysis failed");
        
        // Store the analysis data
        window.comparisonInitialData = analyzeData.analysis;
        
        showStatus("comparisonInitialStatus", "✅ Initial dataset loaded and analyzed", "success");
        
        // Display the results
        displayComparisonAnalysis(analyzeData.analysis, "comparisonInitialResults");
        
        // Enable comparison if final data is also loaded
        if (window.comparisonFinalData) {
            displayComparisonSummary();
        }
        
    } catch (err) {
        console.error('loadInitialExtraction error:', err);
        showStatus("comparisonInitialStatus", "❌ " + err.message, "error");
    }
}

// =====================================================
// FILE UPLOAD HANDLING
// =====================================================
document.addEventListener('DOMContentLoaded', function() {
    const fileInput = document.getElementById('comparisonFileUpload');
    if (fileInput) {
        fileInput.addEventListener('change', handleComparisonFileUpload);
    }
});

function handleComparisonFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    window.comparisonFinalFile = file;
    
    document.getElementById('comparisonFinalFileInfo').innerHTML = `
        <div class="file-info">
            <strong>📄 ${file.name}</strong>
            <small>${(file.size / 1024).toFixed(1)} KB</small>
        </div>
    `;
    
    document.getElementById('analyzeComparisonBtn').disabled = false;
}

// =====================================================
// ANALYZE FINAL DATASET
// =====================================================
async function analyzeComparisonFinal() {
    if (!window.comparisonFinalFile) {
        showStatus("comparisonFinalStatus", "Please upload a file first", "error");
        return;
    }
    
    showStatus("comparisonFinalStatus", "<span class='loading'></span> Analyzing final dataset...", "info");
    
    try {
        const formData = new FormData();
        formData.append('file', window.comparisonFinalFile);
        
        const response = await fetch(`${window.API_BASE}/api/comparison/analyze-final`, {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        console.log('Final dataset analysis:', data);
        
        if (!data.success) throw new Error(data.detail || "Analysis failed");
        
        window.comparisonFinalData = data.analysis;
        
        showStatus("comparisonFinalStatus", "✅ Final dataset analyzed", "success");
        
        // Display the results
        displayComparisonAnalysis(data.analysis, "comparisonFinalResults");
        
        // Show comparison if initial data is also loaded
        if (window.comparisonInitialData) {
            displayComparisonSummary();
        }
        
    } catch (err) {
        console.error('analyzeComparisonFinal error:', err);
        showStatus("comparisonFinalStatus", "❌ " + err.message, "error");
    }
}

// vertical alignment synchronization
function syncRegionHeights() {
    const initialResults = document.getElementById('comparisonInitialResults');
    const finalResults = document.getElementById('comparisonFinalResults');
    
    if (!initialResults || !finalResults) return;
    
    const initialRegions = initialResults.querySelectorAll('.region-section');
    const finalRegions = finalResults.querySelectorAll('.region-section');
    
    // Match heights for each region pair
    const maxLength = Math.max(initialRegions.length, finalRegions.length);
    
    for (let i = 0; i < maxLength; i++) {
        const initial = initialRegions[i];
        const final = finalRegions[i];
        
        if (initial && final) {
            // Reset heights first
            initial.style.height = 'auto';
            final.style.height = 'auto';
            
            // Get natural heights
            const initialHeight = initial.offsetHeight;
            const finalHeight = final.offsetHeight;
            
            // Set both to the taller height
            const maxHeight = Math.max(initialHeight, finalHeight);
            initial.style.minHeight = maxHeight + 'px';
            final.style.minHeight = maxHeight + 'px';
        }
    }
}


// =====================================================
// DISPLAY ANALYSIS RESULTS
// =====================================================
// =====================================================
// DISPLAY ANALYSIS RESULTS (SIDE-BY-SIDE LAYOUT)
// =====================================================
// Update displayComparisonAnalysis function
function displayComparisonAnalysis(analysis, containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    const total = analysis.summary?.total_companies || 0;
    const totalEngland = analysis.summary?.total_england_companies || total;
    const uniqueCounties = analysis.summary?.unique_counties || 0;
    
    let regionalHTML = "";
    
    if (analysis.regional_distribution && analysis.regional_distribution.length) {
        analysis.regional_distribution.forEach(region => {
            let countyRows = "";
            
            if (region.counties && region.counties.length) {
                countyRows = region.counties.map(county => `
                    <div style="display: flex; justify-content: space-between; padding: 5px 10px; border-bottom: 1px solid #ddd;">
                        <span>${county.county}</span>
                        <span><strong>${county.count.toLocaleString()}</strong></span>
                    </div>
                `).join("");
            }
            
            regionalHTML += `
                <div class="region-section" style="margin-bottom: 15px; border: 1px solid #C1E1C1; background: white;">
                    <div style="background: #C1E1C1; padding: 10px; display: flex; justify-content: space-between; font-weight: bold;">
                        <span>${region.region} (${region.region_code})</span>
                        <span>${region.count.toLocaleString()} (${region.percentage})</span>
                    </div>
                    <div style="padding: 5px 0;">
                        ${countyRows}
                    </div>
                </div>
            `;
        });
    }
    
    container.innerHTML = `
        <div style="background: white; padding: 15px; border: 2px solid #C1E1C1; margin-bottom: 15px;">
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; text-align: center;">
                <div>
                    <div style="font-size: 2rem; font-weight: bold; color: #000;">${total.toLocaleString()}</div>
                    <div style="font-size: 0.9rem;">Total</div>
                </div>
                <div>
                    <div style="font-size: 2rem; font-weight: bold; color: #000;">${totalEngland.toLocaleString()}</div>
                    <div style="font-size: 0.9rem;">England</div>
                </div>
                <div>
                    <div style="font-size: 2rem; font-weight: bold; color: #000;">${uniqueCounties}</div>
                    <div style="font-size: 0.9rem;">Counties</div>
                </div>
            </div>
        </div>
        
        <h4 style="margin: 20px 0 10px 0;">Regional Breakdown:</h4>
        ${regionalHTML}
    `;
    
    // Sync heights after rendering - ADD THIS
    setTimeout(() => syncRegionHeights(), 100);
}

// =====================================================
// DISPLAY COMPARISON SUMMARY
// =====================================================
function displayComparisonSummary() {
    const summaryDiv = document.getElementById('comparisonSummary');
    const contentDiv = document.getElementById('comparisonSummaryContent');
    
    if (!window.comparisonInitialData || !window.comparisonFinalData) {
        summaryDiv.style.display = 'none';
        return;
    }
    
    summaryDiv.style.display = 'block';
    
    const initial = window.comparisonInitialData;
    const final = window.comparisonFinalData;
    
    const initialTotal = initial.summary?.total_england_companies || 0;
    const finalTotal = final.summary?.total_england_companies || 0;
    const difference = finalTotal - initialTotal;
    const percentChange = initialTotal > 0 ? ((difference / initialTotal) * 100).toFixed(1) : 0;
    
    // Overall comparison
    let overallHTML = `
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 30px;">
            <div class="stat-card">
                <div class="stat-value">${initialTotal.toLocaleString()}</div>
                <div>Initial Dataset</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${finalTotal.toLocaleString()}</div>
                <div>Final Dataset</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: ${difference >= 0 ? '#48bb78' : '#ff6b6b'};">
                    ${difference >= 0 ? '+' : ''}${difference.toLocaleString()}
                </div>
                <div>Difference</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: ${difference >= 0 ? '#48bb78' : '#ff6b6b'};">
                    ${difference >= 0 ? '+' : ''}${percentChange}%
                </div>
                <div>Change</div>
            </div>
        </div>
    `;
    
    // Region-by-region comparison
    let regionComparisonHTML = '<h3 style="margin-bottom: 15px;">📍 Region-by-Region Comparison</h3>';
    regionComparisonHTML += '<table style="width: 100%; border-collapse: collapse;">';
    regionComparisonHTML += `
        <thead>
            <tr style="background: #C1E1C1; border: 2px solid #000;">
                <th style="padding: 12px; text-align: left; border: 1px solid #000;">Region</th>
                <th style="padding: 12px; text-align: right; border: 1px solid #000;">Initial</th>
                <th style="padding: 12px; text-align: right; border: 1px solid #000;">Final</th>
                <th style="padding: 12px; text-align: right; border: 1px solid #000;">Difference</th>
                <th style="padding: 12px; text-align: right; border: 1px solid #000;">Change %</th>
            </tr>
        </thead>
        <tbody>
    `;
    
    // Create a map of regions for easy comparison
    const initialRegions = {};
    const finalRegions = {};
    
    (initial.regional_distribution || []).forEach(r => {
        initialRegions[r.region] = r.count;
    });
    
    (final.regional_distribution || []).forEach(r => {
        finalRegions[r.region] = r.count;
    });
    
    // Get all unique regions
    const allRegions = new Set([
        ...Object.keys(initialRegions),
        ...Object.keys(finalRegions)
    ]);
    
    allRegions.forEach(region => {
        const initialCount = initialRegions[region] || 0;
        const finalCount = finalRegions[region] || 0;
        const diff = finalCount - initialCount;
        const pctChange = initialCount > 0 ? ((diff / initialCount) * 100).toFixed(1) : 'N/A';
        
        regionComparisonHTML += `
            <tr style="background: white; border: 1px solid #000;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>${region}</strong></td>
                <td style="padding: 10px; text-align: right; border: 1px solid #ddd;">${initialCount.toLocaleString()}</td>
                <td style="padding: 10px; text-align: right; border: 1px solid #ddd;">${finalCount.toLocaleString()}</td>
                <td style="padding: 10px; text-align: right; border: 1px solid #ddd; color: ${diff >= 0 ? '#48bb78' : '#ff6b6b'};">
                    ${diff >= 0 ? '+' : ''}${diff.toLocaleString()}
                </td>
                <td style="padding: 10px; text-align: right; border: 1px solid #ddd; color: ${diff >= 0 ? '#48bb78' : '#ff6b6b'};">
                    ${pctChange !== 'N/A' ? (diff >= 0 ? '+' : '') + pctChange + '%' : 'N/A'}
                </td>
            </tr>
        `;
    });
    
    regionComparisonHTML += '</tbody></table>';
    
    contentDiv.innerHTML = overallHTML + regionComparisonHTML;
}

// =====================================================
// EXPOSE FUNCTIONS GLOBALLY
// =====================================================
window.onComparisonRegionChange = onComparisonRegionChange;
window.clearComparisonFilters = clearComparisonFilters;
window.loadInitialExtraction = loadInitialExtraction;
window.analyzeComparisonFinal = analyzeComparisonFinal;