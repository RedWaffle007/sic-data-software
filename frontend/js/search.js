// =====================================================
// GLOBAL SEARCH FUNCTIONS
// =====================================================

// REMOVED: const declarations that conflict with ui.js
// Using window.API_BASE from ui.js

let lastSearchResults = null;
let searchTimeout = null;

async function globalSearch() {
    const query = document.getElementById("searchQuery").value.trim();
    
    if (query.length < 1) {
        showStatus("searchStatus", "Please enter search term", "error");
        document.getElementById("searchResults").innerHTML = "";
        return;
    }
    
    // Debounce search
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(async () => {
        await executeSearch(query);
    }, 300);
}

async function executeSearch(query) {
    if (query.length < 2) {
        showStatus("searchStatus", "Enter at least 2 characters", "error");
        return;
    }
    
    showStatus("searchStatus", "<span class='loading'></span> Searching across all datasets...", "info");
    
    try {
        const url = `${window.API_BASE}/api/search?q=${encodeURIComponent(query)}`;
        console.log('Search URL:', url);
        
        const res = await fetch(url);
        const data = await res.json();
        
        console.log("Search response:", data);
        
        if (!data.success) throw new Error(data.detail || "Search failed");
        
        lastSearchResults = data;
        
        if (data.total_matching === 0) {
            document.getElementById("searchResults").innerHTML = `
                <div style="text-align:center; padding:40px; color:#666;">
                    <div style="font-size:48px; margin-bottom:20px;">🔍</div>
                    <h3>No results found for "${query}"</h3>
                    <p>Try different keywords or check spelling</p>
                </div>
            `;
            showStatus("searchStatus", "", "");
            return;
        }
        
        renderSearchResults(data, query);
        showStatus("searchStatus", "", "");
        
    } catch (err) {
        console.error("Search error:", err);
        showStatus("searchStatus", err.message, "error");
    }
}

function renderSearchResults(data, query) {
    const datasets = data.datasets || [];
    const totalMatching = data.total_matching || 0;
    
    // Highlight search term in results
    const highlight = (text) => {
        if (!text || !query) return text;
        const regex = new RegExp(`(${query})`, 'gi');
        return text.toString().replace(regex, '<mark class="search-highlight">$1</mark>');
    };
    
    const html = `
        <div style="margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center;">
            <h3 style="margin: 0;">Found ${totalMatching} results</h3>
            <button class="btn-small btn-secondary" onclick="exportSearchResults()" ${totalMatching === 0 ? 'disabled' : ''}>
                📥 Export All Results
            </button>
        </div>
        
        ${datasets.map(ds => `
            <div class="search-dataset-card">
                <div class="search-dataset-header" onclick="toggleDatasetResults(${ds.dataset_id || ds.id})" style="cursor: pointer;">
                    <div>
                        <h4 style="margin: 0; color: #667eea;">
                            ${ds.dataset_name || 'Unnamed Dataset'}
                            <span class="badge">${ds.companies?.length || 0} companies</span>
                        </h4>
                        <small style="color: #666;">
                            Dataset ID: ${ds.dataset_id || ds.id} • 
                            ${ds.companies?.length || 0} of ${totalMatching} results
                        </small>
                    </div>
                    <div class="chevron">▼</div>
                </div>
                
                <div class="search-dataset-companies" id="dataset-${ds.dataset_id || ds.id}" style="display: none;">
                    <div style="display: grid; gap: 10px; margin-top: 15px;">
                        ${(ds.companies || []).map(c => `
                            <div class="search-company-card" 
                                 onclick="navigateToCompany(${ds.dataset_id || ds.id}, '${c.company_number}')">
                                <div class="search-company-header">
                                    <strong>${highlight(c.company_number)}</strong>
                                    <span class="search-badge">${c.county || 'N/A'}</span>
                                </div>
                                <div class="search-company-name">${highlight(c.business_name)}</div>
                                <div class="search-company-details">
                                    ${c.town ? `<span>🏙️ ${highlight(c.town)}</span>` : ''}
                                    ${c.postcode ? `<span>📮 ${highlight(c.postcode)}</span>` : ''}
                                </div>
                                ${c.fname || c.sname ? `
                                    <div class="search-company-details">
                                        👤 ${c.title ? c.title + ' ' : ''}${highlight(c.fname || '')} ${highlight(c.sname || '')}
                                        ${c.position ? ` (${c.position})` : ''}
                                    </div>
                                ` : ''}
                                ${c.selected_person_source ? `
                                    <div class="search-company-meta">
                                        <small>Source: ${c.selected_person_source}</small>
                                        ${c.selected_psc_share_tier ? ` • ${c.selected_psc_share_tier}` : ''}
                                    </div>
                                ` : ''}
                                <div class="search-company-footer">
                                    <small>Click to view in dataset</small>
                                </div>
                            </div>
                        `).join("")}
                    </div>
                </div>
            </div>
        `).join("")}
    `;
    
    document.getElementById("searchResults").innerHTML = html;
}

function toggleDatasetResults(datasetId) {
    const element = document.getElementById(`dataset-${datasetId}`);
    if (!element) return;
    
    const chevron = element.previousElementSibling.querySelector('.chevron');
    if (!chevron) return;
    
    if (element.style.display === 'none') {
        element.style.display = 'block';
        chevron.textContent = '▲';
    } else {
        element.style.display = 'none';
        chevron.textContent = '▼';
    }
}

function exportSearchResults() {
    if (!lastSearchResults || lastSearchResults.total_matching === 0) {
        alert('No results to export');
        return;
    }
    
    const query = document.getElementById("searchQuery").value.trim();
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `search_${query}_${timestamp}.csv`;
    
    // Create CSV content
    let csvContent = "CompanyNumber,BusinessName,County,Postcode,Town,Title,FirstName,Surname,Position,SelectedPersonSource,ShareTier,Dataset\n";
    
    lastSearchResults.datasets.forEach(ds => {
        ds.companies?.forEach(c => {
            const row = [
                `"${c.company_number || ''}"`,
                `"${c.business_name || ''}"`,
                `"${c.county || ''}"`,
                `"${c.postcode || ''}"`,
                `"${c.town || ''}"`,
                `"${c.title || ''}"`,
                `"${c.fname || ''}"`,
                `"${c.sname || ''}"`,
                `"${c.position || ''}"`,
                `"${c.selected_person_source || ''}"`,
                `"${c.selected_psc_share_tier || ''}"`,
                `"${ds.dataset_name || ''}"`
            ];
            csvContent += row.join(',') + '\n';
        });
    });
    
    // Create download link
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
}

async function navigateToCompany(datasetId, companyNumber) {
    console.log(`Navigating to company ${companyNumber} in dataset ${datasetId}`);
    
    switchTab('datasets');
    
    setTimeout(async () => {
        try {
            // viewDataset is in datasets.js
            if (typeof viewDataset === 'function') {
                await viewDataset(datasetId);
                
                setTimeout(() => {
                    const cells = document.querySelectorAll('[data-field="company_number"]');
                    for (const cell of cells) {
                        if (cell.textContent.trim() === companyNumber) {
                            cell.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            const row = cell.closest('tr');
                            row.style.background = '#fff3cd';
                            
                            setTimeout(() => {
                                row.style.background = '';
                            }, 3000);
                            return;
                        }
                    }
                    alert(`Company ${companyNumber} is in the dataset. You may need to scroll.`);
                }, 500);
            } else {
                alert(`Cannot navigate - viewDataset function not found`);
            }
            
        } catch (error) {
            console.error('Navigation error:', error);
            alert('Could not navigate to company.');
        }
    }, 100);
}

// Add real-time search as user types
document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.getElementById('searchQuery');
    if (searchInput) {
        searchInput.addEventListener('input', globalSearch);
    }
});

// Make functions globally available
window.globalSearch = globalSearch;
window.toggleDatasetResults = toggleDatasetResults;
window.exportSearchResults = exportSearchResults;
window.navigateToCompany = navigateToCompany;