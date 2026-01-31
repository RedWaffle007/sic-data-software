// =====================================================
// LETTER GENERATION FUNCTIONS
// =====================================================

console.log('=== LETTERS.JS LOADED ===');

// Global letter state
window.currentLetterFile = null;
window.currentTemplateFile = null;
window.letterFunctionsInitialized = false;

// Initialize letter generation UI
function initLetterGeneration() {
    console.log('=== INIT LETTER GENERATION ===');
    
    if (window.letterFunctionsInitialized) {
        console.log('Letter functions already initialized');
        return;
    }
    
    // Get elements
    const lettersPerFileSection = document.getElementById('lettersPerFileSection');
    const lettersPerFileInput = document.getElementById('lettersPerFile');
    const outputModeRadios = document.querySelectorAll('input[name="outputMode"]');
    const dataSourceRadios = document.querySelectorAll('input[name="dataSource"]');
    
    console.log('Letter generation elements found:', {
        lettersPerFileSection: !!lettersPerFileSection,
        lettersPerFileInput: !!lettersPerFileInput,
        outputModeRadios: outputModeRadios.length,
        dataSourceRadios: dataSourceRadios.length
    });
    
    // REMOVE ANY INLINE STYLES that might be hiding elements
    if (lettersPerFileSection) {
        lettersPerFileSection.style.display = '';
        lettersPerFileSection.style.visibility = '';
        console.log('✓ Removed inline styles from lettersPerFileSection');
    }
    
    // Set up event listeners for output mode
    outputModeRadios.forEach(radio => {
        radio.removeEventListener('change', handleOutputModeChange);
        radio.addEventListener('change', handleOutputModeChange);
        
        const label = radio.closest('.toggle-option');
        if (label) {
            label.removeEventListener('click', handleToggleOptionClick);
            label.addEventListener('click', handleToggleOptionClick);
        }
    });
    
    // Set up event listeners for data source
    dataSourceRadios.forEach(radio => {
        radio.removeEventListener('change', handleDataSourceChange);
        radio.addEventListener('change', handleDataSourceChange);
        
        const label = radio.closest('.toggle-option');
        if (label) {
            label.removeEventListener('click', handleToggleOptionClick);
            label.addEventListener('click', handleToggleOptionClick);
        }
    });
    
    // File upload listeners - FIXED: Direct event listener, not via onchange attribute
    const fileUpload = document.getElementById('letterFileUpload');
    if (fileUpload) {
        fileUpload.removeEventListener('change', handleLetterFileUpload);
        fileUpload.addEventListener('change', handleLetterFileUpload);
        console.log('✓ Added change listener to letterFileUpload');
    }
    
    const templateUpload = document.getElementById('templateFileUpload');
    if (templateUpload) {
        templateUpload.removeEventListener('change', handleTemplateFileUpload);
        templateUpload.addEventListener('change', handleTemplateFileUpload);
        console.log('✓ Added change listener to templateFileUpload');
    }
    
    // Set initial state based on default selection
    const defaultMode = document.querySelector('input[name="outputMode"]:checked');
    console.log('Initial output mode:', defaultMode ? defaultMode.value : 'none');
    updateOutputModeUI();
    
    // Set initial data source
    const defaultDataSource = document.querySelector('input[name="dataSource"]:checked');
    if (defaultDataSource) {
        updateDataSourceUI(defaultDataSource.value);
    }
    
    window.letterFunctionsInitialized = true;
    console.log('✓ Letter generation initialized');
}

function handleToggleOptionClick(e) {
    const radio = this.querySelector('input[type="radio"]');
    if (radio) {
        radio.checked = true;
        const changeEvent = new Event('change');
        radio.dispatchEvent(changeEvent);
    }
}

function handleOutputModeChange(e) {
    console.log('Output mode changed to:', e.target.value);
    updateOutputModeUI();
}

function handleDataSourceChange(e) {
    console.log('Data source changed to:', e.target.value);
    updateDataSourceUI(e.target.value);
}

function updateOutputModeUI() {
    const checkedRadio = document.querySelector('input[name="outputMode"]:checked');
    if (!checkedRadio) {
        console.error('No output mode selected');
        return;
    }
    
    const mode = checkedRadio.value;
    const lettersPerFileSection = document.getElementById('lettersPerFileSection');
    const outputModeLabel = document.getElementById('outputModeLabel');
    
    console.log('Updating output mode UI for:', mode);
    
    // Update active state on toggle options
    document.querySelectorAll('#outputModeToggle .toggle-option').forEach(option => {
        option.classList.remove('active');
    });
    checkedRadio.closest('.toggle-option').classList.add('active');
    
    if (mode === 'combined') {
        if (lettersPerFileSection) {
            lettersPerFileSection.classList.remove('hidden');
            lettersPerFileSection.style.display = 'block';
            lettersPerFileSection.style.visibility = 'visible';
            lettersPerFileSection.style.opacity = '1';
            console.log('✓ Showing letters per file section');
        }
        
        if (outputModeLabel) {
            outputModeLabel.textContent = 'ZIP with N letters per DOCX file (customize below)';
        }
    } else {
        if (lettersPerFileSection) {
            lettersPerFileSection.classList.add('hidden');
            console.log('✓ Hiding letters per file section');
        }
        
        if (outputModeLabel) {
            outputModeLabel.textContent = 'ZIP archive with one letter per DOCX file';
        }
    }
}

function updateDataSourceUI(source) {
    console.log('Updating data source UI for:', source);
    
    const uploadSection = document.getElementById('uploadSection');
    const datasetSection = document.getElementById('datasetSection');
    
    // Update active state on toggle options
    document.querySelectorAll('#dataSourceToggle .toggle-option').forEach(option => {
        option.classList.remove('active');
    });
    
    const activeRadio = document.querySelector(`input[name="dataSource"][value="${source}"]`);
    if (activeRadio && activeRadio.closest('.toggle-option')) {
        activeRadio.closest('.toggle-option').classList.add('active');
    }
    
    if (source === 'upload') {
        if (uploadSection) {
            uploadSection.classList.remove('hidden');
            uploadSection.style.display = 'block';
        }
        if (datasetSection) {
            datasetSection.classList.add('hidden');
            datasetSection.style.display = 'none';
        }
        console.log('✓ Showing upload section');
    } else {
        if (uploadSection) {
            uploadSection.classList.add('hidden');
            uploadSection.style.display = 'none';
        }
        if (datasetSection) {
            datasetSection.classList.remove('hidden');
            datasetSection.style.display = 'block';
        }
        console.log('✓ Showing dataset section');
    }
}

function handleLetterFileUpload(e) {
    console.log('=== handleLetterFileUpload TRIGGERED ===');
    
    const file = e.target.files[0];
    const fileInfo = document.getElementById('fileInfo');
    
    if (!file) {
        console.log('No file selected, clearing display');
        window.currentLetterFile = null;
        if (fileInfo) {
            fileInfo.innerHTML = '';
        }
        return;
    }
    
    const validTypes = ['.csv', '.xlsx', '.xls'];
    const fileExt = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
    
    if (!validTypes.some(ext => fileExt === ext)) {
        showLetterStatus('Error: Please upload a CSV or Excel file (.csv, .xlsx, .xls)', 'error');
        e.target.value = '';
        window.currentLetterFile = null;
        if (fileInfo) {
            fileInfo.innerHTML = '';
        }
        return;
    }
    
    window.currentLetterFile = file;
    
    if (fileInfo) {
        fileInfo.innerHTML = `
            <div style="background: #C1E1C1; padding: 12px; border: 2px solid #000; margin-top: 10px; border-radius: 0; position: relative;">
                <button onclick="clearDataFile()" style="position: absolute; top: 10px; right: 10px; background: #ff6b6b; color: white; border: 2px solid #000; padding: 4px 12px; cursor: pointer; font-size: 12px; font-weight: bold; border-radius: 0;">
                    ✕ Clear
                </button>
                <div style="font-size: 1.5rem; margin-bottom: 5px;">📄</div>
                <strong>${file.name}</strong><br>
                <small>${formatFileSize(file.size)} • Ready to process ALL rows</small>
                <div style="margin-top: 5px; font-size: 0.9em;">
                    <span style="background: #28a745; color: white; padding: 2px 8px; border-radius: 10px; margin-right: 5px;">
                        ${fileExt.toUpperCase()}
                    </span>
                    <span style="background: #6c757d; color: white; padding: 2px 8px; border-radius: 10px;">
                        ${new Date().toLocaleTimeString()}
                    </span>
                </div>
            </div>
        `;
        console.log('✓ Data file upload UI updated:', file.name);
    }
    
    showLetterStatus(`Data file loaded: ${file.name} (${formatFileSize(file.size)})`, 'success');
}

function handleTemplateFileUpload(e) {
    console.log('=== handleTemplateFileUpload TRIGGERED ===');
    
    const file = e.target.files[0];
    const templateInfo = document.getElementById('templateInfo');
    
    if (!file) {
        console.log('No file selected, clearing display');
        window.currentTemplateFile = null;
        if (templateInfo) {
            templateInfo.innerHTML = '';
        }
        return;
    }
    
    const fileExt = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
    
    if (fileExt !== '.docx') {
        showLetterStatus('Error: Template must be a .docx file', 'error');
        e.target.value = '';
        window.currentTemplateFile = null;
        if (templateInfo) {
            templateInfo.innerHTML = '';
        }
        return;
    }
    
    window.currentTemplateFile = file;
    
    if (templateInfo) {
        templateInfo.innerHTML = `
            <div style="background: #D1F1D1; padding: 12px; border: 2px solid #000; margin-top: 10px; border-radius: 0; position: relative;">
                <button onclick="clearTemplateFile()" style="position: absolute; top: 10px; right: 10px; background: #ff6b6b; color: white; border: 2px solid #000; padding: 4px 12px; cursor: pointer; font-size: 12px; font-weight: bold; border-radius: 0;">
                    ✕ Clear
                </button>
                <div style="font-size: 1.5rem; margin-bottom: 5px;">📝</div>
                <strong>Template: ${file.name}</strong><br>
                <small>${formatFileSize(file.size)} • Will use this template for generation</small>
                <div style="margin-top: 5px; font-size: 0.9em;">
                    <span style="background: #007bff; color: white; padding: 2px 8px; border-radius: 10px; margin-right: 5px;">
                        DOCX
                    </span>
                    <span style="background: #6c757d; color: white; padding: 2px 8px; border-radius: 10px;">
                        ${new Date().toLocaleTimeString()}
                    </span>
                </div>
            </div>
        `;
        console.log('✓ Template upload UI updated:', file.name);
    }
    
    showLetterStatus(`Template loaded: ${file.name}`, 'success');
}

// NEW: Separate clear functions for data file and template
function clearDataFile() {
    console.log('Clearing data file');
    const fileUpload = document.getElementById('letterFileUpload');
    const fileInfo = document.getElementById('fileInfo');
    
    if (fileUpload) {
        fileUpload.value = '';
    }
    if (fileInfo) {
        fileInfo.innerHTML = '';
    }
    
    window.currentLetterFile = null;
    showLetterStatus('Data file cleared', 'info');
}

function clearTemplateFile() {
    console.log('Clearing template file');
    const templateUpload = document.getElementById('templateFileUpload');
    const templateInfo = document.getElementById('templateInfo');
    
    if (templateUpload) {
        templateUpload.value = '';
    }
    if (templateInfo) {
        templateInfo.innerHTML = '';
    }
    
    window.currentTemplateFile = null;
    showLetterStatus('Template file cleared', 'info');
}

async function generateLetters() {
    console.log('generateLetters called');
    
    // Check data source
    const dataSource = document.querySelector('input[name="dataSource"]:checked');
    if (!dataSource) {
        showLetterStatus('Please select a data source', 'error');
        return;
    }
    
    if (dataSource.value === 'upload') {
        await generateFromUpload();
    } else {
        await generateFromDataset();
    }
}

async function generateFromUpload() {
    const fileUpload = document.getElementById('letterFileUpload');
    if (!fileUpload.files.length) {
        showLetterStatus('Please select a data file', 'error');
        return;
    }
    
    const file = fileUpload.files[0];
    
    // MANDATORY template check
    const templateUpload = document.getElementById('templateFileUpload');
    if (!templateUpload.files.length) {
        showLetterStatus('Please upload a template file (.docx) - this is mandatory', 'error');
        return;
    }
    
    const templateFile = templateUpload.files[0];
    
    const outputMode = document.querySelector('input[name="outputMode"]:checked');
    if (!outputMode) {
        showLetterStatus('Please select an output mode', 'error');
        return;
    }
    
    let lettersPerFile = 1;
    
    if (outputMode.value === 'combined') {
        const lettersPerFileInput = document.getElementById('lettersPerFile');
        if (!lettersPerFileInput) {
            showLetterStatus('Letters per file input not found', 'error');
            return;
        }
        
        lettersPerFile = parseInt(lettersPerFileInput.value);
        if (isNaN(lettersPerFile) || lettersPerFile < 1) {
            showLetterStatus('Letters per file must be a number greater than 0', 'error');
            return;
        }
        
        console.log('Combined mode with letters per file:', lettersPerFile);
    }
    
    console.log('Generating letters:', {
        filename: file.name,
        mode: outputMode.value,
        lettersPerFile: lettersPerFile,
        size: file.size,
        templateFile: templateFile.name
    });
    
    showLetterStatus('<span class="loading"></span> Processing file and generating letters...', 'info');
    
    const formData = new FormData();
    formData.append('file', file);
    formData.append('mode', outputMode.value);
    formData.append('letters_per_file', lettersPerFile.toString());
    formData.append('template', templateFile);
    
    console.log('Sending template:', templateFile.name);
    
    try {
        const response = await fetch('/api/letters/generate/upload', {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(errorData.error || `Generation failed (${response.status})`);
        }
        
        // Handle file download
        const contentDisposition = response.headers.get('content-disposition');
        let filename = 'letters.zip';
        
        if (contentDisposition) {
            const match = contentDisposition.match(/filename="?([^"]+)"?/i);
            if (match && match[1]) filename = match[1];
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        const totalLetters = response.headers.get('X-Total-Letters') || 'unknown';
        const filesCreated = response.headers.get('X-Files-Created') || 'unknown';
        
        showLetterStatus(`✓ Successfully generated ${totalLetters} letters in ${filesCreated} file(s)! Check your Downloads folder.`, 'success');
        
    } catch (error) {
        console.error('Generation error:', error);
        showLetterStatus(`Error: ${error.message}`, 'error');
    }
}

async function generateFromDataset() {
    const datasetSelect = document.getElementById('datasetSelect');
    if (!datasetSelect || !datasetSelect.value) {
        showLetterStatus('Please select a dataset', 'error');
        return;
    }
    
    // MANDATORY template check for dataset generation too
    const templateUpload = document.getElementById('templateFileUpload');
    if (!templateUpload.files.length) {
        showLetterStatus('Please upload a template file (.docx) - this is mandatory', 'error');
        return;
    }
    
    const outputMode = document.querySelector('input[name="outputMode"]:checked');
    if (!outputMode) {
        showLetterStatus('Please select an output mode', 'error');
        return;
    }
    
    let lettersPerFile = 1;
    
    if (outputMode.value === 'combined') {
        const lettersPerFileInput = document.getElementById('lettersPerFile');
        if (!lettersPerFileInput) {
            showLetterStatus('Letters per file input not found', 'error');
            return;
        }
        
        lettersPerFile = parseInt(lettersPerFileInput.value);
        if (isNaN(lettersPerFile) || lettersPerFile < 1) {
            showLetterStatus('Letters per file must be a number greater than 0', 'error');
            return;
        }
    }
    
    showLetterStatus(`<span class="loading"></span> Generating letters from dataset: ${datasetSelect.value}...`, 'info');
    
    try {
        const response = await fetch(`/api/letters/generate/dataset/${datasetSelect.value}?mode=${outputMode.value}&letters_per_file=${lettersPerFile}`, {
            method: 'POST',
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(errorData.error || `Generation failed (${response.status})`);
        }
        
        // Handle file download
        const contentDisposition = response.headers.get('content-disposition');
        let filename = 'letters.zip';
        
        if (contentDisposition) {
            const match = contentDisposition.match(/filename="?([^"]+)"?/i);
            if (match && match[1]) filename = match[1];
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        showLetterStatus(`✓ Successfully generated letters from dataset! Check your Downloads folder.`, 'success');
        
    } catch (error) {
        console.error('Generation error:', error);
        showLetterStatus(`Error: ${error.message}`, 'error');
    }
}

function showLetterStatus(message, type = 'info') {
    const statusDiv = document.getElementById('letterStatus');
    if (!statusDiv) return;
    
    if (!message) {
        statusDiv.style.display = 'none';
        return;
    }
    
    // Create HTML based on message type
    let html = '';
    if (type === 'success') {
        html = `<div style="color: #155724; background-color: #d4edda; border-color: #c3e6cb; padding: 10px; border-radius: 4px;">
                   ${message}
                </div>`;
    } else if (type === 'error') {
        html = `<div style="color: #721c24; background-color: #f8d7da; border-color: #f5c6cb; padding: 10px; border-radius: 4px;">
                   ${message}
                </div>`;
    } else if (type === 'warning') {
        html = `<div style="color: #856404; background-color: #fff3cd; border-color: #ffeaa7; padding: 10px; border-radius: 4px;">
                   ${message}
                </div>`;
    } else {
        html = `<div style="color: #004085; background-color: #cce5ff; border-color: #b8daff; padding: 10px; border-radius: 4px;">
                   ${message}
                </div>`;
    }
    
    statusDiv.innerHTML = html;
    statusDiv.style.display = 'block';
    
    if (type === 'success' || type === 'error') {
        setTimeout(() => {
            statusDiv.style.display = 'none';
        }, 8000); // Increased to 8 seconds so user can read the success message
    }
}

function clearLetterForm() {
    // Clear data file upload
    clearDataFile();
    
    // Clear template upload
    clearTemplateFile();
    
    // Reset letters per file to default
    document.getElementById('lettersPerFile').value = '5';
    
    // Reset to default selections
    const uploadRadio = document.querySelector('input[name="dataSource"][value="upload"]');
    const zipRadio = document.querySelector('input[name="outputMode"][value="zip"]');
    
    if (uploadRadio) uploadRadio.checked = true;
    if (zipRadio) zipRadio.checked = true;
    
    // Update UI
    updateOutputModeUI();
    updateDataSourceUI('upload');
    
    showLetterStatus('', 'info');
    
    console.log('✓ Form cleared completely');
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// =====================================================
// AUTO-INITIALIZATION CODE
// =====================================================

// Check if we should initialize on page load
function checkAndInitializeLetters() {
    const lettersTab = document.getElementById('content-letters');
    if (lettersTab && lettersTab.classList.contains('active')) {
        console.log('Letters tab is active, initializing letter generation...');
        if (typeof initLetterGeneration === 'function') {
            setTimeout(() => {
                initLetterGeneration();
                console.log('✓ Letter generation initialized on page load');
            }, 300);
        }
    }
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
        console.log('DOM ready in letters.js');
        setTimeout(checkAndInitializeLetters, 500);
    });
} else {
    console.log('DOM already loaded in letters.js');
    setTimeout(checkAndInitializeLetters, 500);
}

// Export functions to global scope
window.initLetterGeneration = initLetterGeneration;
window.generateLetters = generateLetters;
window.clearLetterForm = clearLetterForm;
window.clearDataFile = clearDataFile;
window.clearTemplateFile = clearTemplateFile;
window.updateOutputModeUI = updateOutputModeUI;
window.updateDataSourceUI = updateDataSourceUI;

console.log('✓ Letters.js functions exported');
console.log('=== LETTERS.JS READY ===');