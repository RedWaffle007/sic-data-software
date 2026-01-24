// =====================================================
// GLOBAL UI STATE & CONSTANTS (MASTER FILE)
// =====================================================

// Define API_BASE ONCE
if (!window.API_BASE) {
    window.API_BASE = window.location.origin;
    console.log('UI.js: API_BASE set to', window.API_BASE);
}

// Define global state ONCE
window.currentJobId = window.currentJobId || null;
window.enrichmentJobId = window.enrichmentJobId || null;
window.currentDatasetFile = window.currentDatasetFile || null;
window.currentSicCodes = window.currentSicCodes || [];
window.currentCounties = window.currentCounties || null;
window.currentViewDatasetId = window.currentViewDatasetId || null;

// =====================================================
// TAB SWITCHING
// =====================================================
function switchTab(tab) {
    console.log('Switching to tab:', tab);
    
    // Hide all tab contents
    document.querySelectorAll(".tab-content").forEach(el => {
        el.classList.remove("active");
    });
    
    // Deactivate all tab buttons
    document.querySelectorAll(".nav-tabs button").forEach(el => {
        el.classList.remove("active");
    });
    
    // Show selected tab content
    const contentElement = document.getElementById(`content-${tab}`);
    if (contentElement) {
        contentElement.classList.add("active");
    }
    
    // Activate selected tab button
    const tabButton = document.getElementById(`tab-${tab}`);
    if (tabButton) {
        tabButton.classList.add("active");
    }
    
    // Load datasets if on datasets tab
    if (tab === "datasets" && typeof window.loadDatasets === "function") {
        console.log('Auto-loading datasets...');
        window.loadDatasets();
    }
}

// =====================================================
// STATUS HELPER
// =====================================================
function showStatus(id, message, type = "") {
    const el = document.getElementById(id);
    if (!el) {
        console.error('showStatus: Element not found:', id);
        return;
    }
    
    if (!message) {
        el.className = "status-box";
        el.innerHTML = "";
        el.style.display = 'none';
        return;
    }
    
    el.className = `status-box ${type}`;
    el.innerHTML = message;
    el.style.display = 'block';
}

// =====================================================
// MODAL FUNCTIONS
// =====================================================
function showSaveModal() {
    console.log('Showing save modal');
    document.getElementById('saveModal').style.display = 'block';
}

function closeSaveModal() {
    console.log('Closing save modal');
    document.getElementById('saveModal').style.display = 'none';
    document.getElementById('saveModalStatus').innerHTML = '';
    document.getElementById('saveDatasetName').value = '';
    document.getElementById('saveDatasetDesc').value = '';
}

// =====================================================
// DATASET NAVIGATION
// =====================================================
function backToDatasetList() {
    console.log('Back to dataset list');
    document.querySelector("#content-datasets > .card").style.display = "block";
    document.getElementById("datasetDetailCard").style.display = "none";
    window.currentViewDatasetId = null;
}

// =====================================================
// ANALYSIS DISPLAY HELPER
// =====================================================
function displayAnalysis(a) {
    // Ensure we have valid data
    if (!a || typeof a !== 'object') {
        return '<p style="color:#999; padding:20px;">No analysis data available</p>';
    }
    
    let regionalHTML = "";
    
    if (a.regional_distribution && a.regional_distribution.length) {
        a.regional_distribution.forEach(region => {
            let countyRows = "";
            
            if (region.counties && region.counties.length) {
                countyRows = region.counties.map(county => `
                    <div class="county-row">
                        <span>${county.county}</span>
                        <span><strong>${county.count.toLocaleString()}</strong> (${county.percentage})</span>
                    </div>
                `).join("");
            }
            
            regionalHTML += `
                <div class="region-section">
                    <div class="region-header">
                        <span><strong>${region.region}</strong> (${region.region_code})</span>
                        <span>${region.count.toLocaleString()} companies (${region.percentage})</span>
                    </div>
                    <div class="region-counties">
                        ${countyRows}
                    </div>
                </div>
            `;
        });
    } else if (a.county_distribution && a.county_distribution.length) {
        regionalHTML = `
            <table>
                <thead>
                    <tr>
                        <th>County</th>
                        <th>Companies</th>
                        <th>Percentage</th>
                    </tr>
                </thead>
                <tbody>
                    ${a.county_distribution.map(row => `
                        <tr>
                            <td>${row.county}</td>
                            <td>${row.count.toLocaleString()}</td>
                            <td>${row.percentage}</td>
                        </tr>
                    `).join("")}
                </tbody>
            </table>
        `;
    } else {
        regionalHTML = `<p style="color: #999; padding: 20px; text-align: center;">No regional data available</p>`;
    }
    
    const resolution = a.county_resolution || {};
    const missing = a.missing_data || {};
    const total = a.summary?.total_companies || 0;
    const totalEngland = a.summary?.total_england_companies || total;
    
    return `
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value">${total.toLocaleString()}</div>Total</div>
            <div class="stat-card"><div class="stat-value">${totalEngland.toLocaleString()}</div>England</div>
            <div class="stat-card"><div class="stat-value">${a.data_quality_score || 0}</div>Quality</div>
            <div class="stat-card"><div class="stat-value">${a.summary?.unique_counties || "N/A"}</div>Counties</div>
        </div>

        <h3 style="margin-top: 25px;">📊 Resolution Breakdown</h3>
        <div class="stats-grid">
            <div class="stat-card">
                <strong>Provided by Source Data</strong><br>
                ${resolution.direct_from_csv ?? "N/A"}
                ${resolution.direct_from_csv != null && totalEngland > 0 ? 
                    `(${((resolution.direct_from_csv / totalEngland) * 100).toFixed(1)}%)` : ""}
            </div>

            <div class="stat-card">
                <strong>Derived from Postcode</strong><br>
                ${resolution.resolved_from_postcode ?? "N/A"}
                ${resolution.resolved_from_postcode != null && totalEngland > 0 ? 
                    `(${((resolution.resolved_from_postcode / totalEngland) * 100).toFixed(1)}%)` : ""}
            </div>

            <div class="stat-card">
                <strong>Location Unknown</strong><br>
                ${resolution.unresolvable ?? "N/A"}
                ${resolution.unresolvable != null && totalEngland > 0 ? 
                    `(${((resolution.unresolvable / totalEngland) * 100).toFixed(1)}%)` : ""}
            </div>
        </div>

        <h3 style="margin-top: 25px;">⚠️ Data Loss</h3>
        <div class="stats-grid">
            <div class="stat-card"><strong>Missing Postcodes</strong><br>${missing.postcode_missing ?? "N/A"}</div>
            <div class="stat-card"><strong>Missing Counties</strong><br>${missing.county_missing ?? "N/A"}</div>
        </div>

        <h3 style="margin-top: 25px;">📍 England Regions & Counties</h3>
        ${regionalHTML}

        <h3 style="margin-top: 25px;">🧬 Dataset Lineage</h3>
        <p><strong>Source:</strong> ${a.summary?.dataset_file || "Company dataset"}</p>
        <p><strong>Processed On:</strong> ${a.summary?.analysis_timestamp || new Date().toLocaleString()}</p>
    `;
}

// =====================================================
// COMPANY TABLE (SIMPLE INLINE EDITING - NO EXCEL)
// =====================================================
function renderCompaniesTable(containerId, companies, datasetId) {
    const container = document.getElementById(containerId);
    if (!container) {
        console.error('renderCompaniesTable: Container not found:', containerId);
        return;
    }
    
    if (!companies || companies.length === 0) {
        container.innerHTML = "<p style='color:#999;'>No companies found.</p>";
        return;
    }
    
    console.log(`Rendering ${companies.length} companies for dataset ${datasetId}`);
    
    const columns = [
        { key: "company_number", label: "CompanyNumber", editable: false },
        { key: "business_name", label: "BusinessName", editable: true },
        { key: "address_line1", label: "AddressLine1", editable: true },
        { key: "address_line2", label: "AddressLine2", editable: true },
        { key: "town", label: "Town", editable: true },
        { key: "county", label: "County", editable: true },
        { key: "postcode", label: "Postcode", editable: true },
        { key: "person_with_significant_control", label: "PersonWithSignificantControl", editable: false },
        { key: "nature_of_control", label: "NatureOfControl", editable: false },
        { key: "title", label: "Title", editable: true },
        { key: "fname", label: "Fname", editable: true },
        { key: "sname", label: "Sname", editable: true },
        { key: "position", label: "Position", editable: true },
        { key: "sic", label: "SIC", editable: false },
        { key: "company_status", label: "CompanyStatus", editable: false },
        { key: "company_type", label: "CompanyType", editable: false },
        { key: "date_of_creation", label: "DateOfCreation", editable: false },
        { key: "website", label: "Website", editable: true },
        { key: "phone", label: "Phone", editable: true },
        { key: "email", label: "Email", editable: true },
        { key: "website_address", label: "WebsiteAddress", editable: true },
        { key: "address_match", label: "AddressMatch(RegVsWeb)", editable: true }
    ];
    
    const thead = `
        <thead>
            <tr>${columns.map(c => `<th>${c.label}</th>`).join("")}</tr>
        </thead>
    `;
    
    const tbody = `
        <tbody>
            ${companies.slice(0, 50).map(c => `
                <tr>
                    ${columns.map(col => `
                        <td
                            ${col.editable ? "contenteditable='true'" : ""}
                            data-company-id="${c.id}"
                            data-field="${col.key}"
                            class="${col.editable ? "editable-cell" : "readonly-cell"}"
                            data-dataset-id="${datasetId}"
                        >
                            ${c[col.key] ?? ""}
                        </td>
                    `).join("")}
                </tr>
            `).join("")}
        </tbody>
    `;
    
    let footer = '';
    if (companies.length > 50) {
        footer = `<p style="text-align: center; color: #666; padding: 10px;">
            Showing 50 of ${companies.length} companies
        </p>`;
    }
    
    container.innerHTML = `
        <div style="overflow-x:auto;">
            <table id="company-table-${datasetId}">
                ${thead}
                ${tbody}
            </table>
            ${footer}
        </div>
    `;
    
    // Enable inline editing
    enableInlineEditing(datasetId);
}

// =====================================================
// SIMPLE INLINE EDITING (NO EXCEL FEATURES)
// =====================================================
function enableInlineEditing(datasetId) {
    document.querySelectorAll(".editable-cell").forEach(cell => {
        // Store original value
        cell.dataset.originalValue = cell.innerText.trim();
        
        cell.addEventListener("blur", () => {
            saveCell(cell, datasetId);
        });
        
        cell.addEventListener("keydown", e => {
            if (e.key === "Enter") {
                e.preventDefault();
                cell.blur();
            }
        });
    });
}

async function saveCell(cell, datasetId) {
    const companyId = cell.dataset.companyId;
    const field = cell.dataset.field;
    const value = cell.innerText.trim();
    
    // Save original value in case of failure
    const originalValue = cell.dataset.originalValue || cell.innerText;
    
    // Visual feedback - editing state
    cell.style.background = "#fff3cd";
    cell.classList.add('editing');
    
    try {
        console.log(`Saving company ${companyId}, field ${field}: ${value}`);
        
        const res = await fetch(
            `${window.API_BASE}/api/companies/${companyId}`,
            {
                method: "PATCH",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ [field]: value })
            }
        );
        
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }
        
        const data = await res.json();
        if (!data.success) throw new Error("Save failed");
        
        // Success feedback
        cell.style.background = "#d4edda";
        cell.classList.remove('editing');
        cell.classList.add('saved');
        cell.dataset.originalValue = value; // Update original value
        
        // Remove saved class after animation
        setTimeout(() => {
            cell.classList.remove('saved');
            cell.style.background = "";
        }, 1000);
        
    } catch (err) {
        console.error("Save failed:", err);
        
        // Revert to original value
        cell.innerText = originalValue;
        cell.style.background = "#f8d7da";
        cell.classList.remove('editing');
        cell.classList.add('error');
        
        setTimeout(() => {
            cell.classList.remove('error');
            cell.style.background = "";
        }, 3000);
    }
}

// =====================================================
// INITIALIZATION
// =====================================================
document.addEventListener('DOMContentLoaded', () => {
    console.log('UI.js initialized');
});