document.addEventListener('DOMContentLoaded', () => {
    // Dynamic WebSocket URL based on current host
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    const ws = new WebSocket(wsUrl);
    let reconnectInterval = null;

    // Throttling for dashboard updates
    let updatePending = false;
    let lastUpdate = 0;
    const UPDATE_THROTTLE_MS = 250; // Update at most 4 times per second
    
    // Result pagination - define as window properties for global access
    window.allResults = [];
    window.currentResultPage = 1;
    window.RESULTS_PER_PAGE = 20;
    
    // Operation management state
    let activeOperationId = null;
    let operationState = 'idle';

    ws.onopen = () => {
        console.log('WebSocket connected');
        updateConnectionStatus('connected');
        if (reconnectInterval) {
            clearInterval(reconnectInterval);
            reconnectInterval = null;
        }
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        
        // Throttle updates for high-frequency data
        const now = Date.now();
        if (now - lastUpdate < UPDATE_THROTTLE_MS) {
            if (!updatePending) {
                updatePending = true;
                setTimeout(() => {
                    updatePending = false;
                    updateDashboard(data);
                    lastUpdate = Date.now();
                }, UPDATE_THROTTLE_MS - (now - lastUpdate));
            }
        } else {
            updateDashboard(data);
            lastUpdate = now;
        }
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected');
        updateConnectionStatus('disconnected');
        // Attempt to reconnect every 5 seconds
        if (!reconnectInterval) {
            reconnectInterval = setInterval(() => {
                location.reload();
            }, 5000);
        }
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        updateConnectionStatus('error');
    };

    // Setup operation form
    const operationForm = document.getElementById('operation-form');
    operationForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        const submitButton = e.target.querySelector('button[type="submit"]');
        submitButton.disabled = true;
        submitButton.textContent = 'Creating...';
        
        const formData = {
            name: document.getElementById('op-name').value,
            description: document.getElementById('op-description').value,
            targets: document.getElementById('op-targets').value.split('\n').filter(t => t.trim()),
            usernames: document.getElementById('op-usernames').value.split('\n').filter(u => u.trim()),
            passwords: document.getElementById('op-passwords').value.split('\n').filter(p => p.trim()),
            priority: parseInt(document.getElementById('op-priority').value),
            allow_graceful_shutdown: document.getElementById('op-graceful').checked
        };

        try {
            const response = await fetch('/api/operations/create', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(formData),
            });
            
            const result = await response.json();
            
            if (response.ok) {
                showNotification(`Operation "${result.name}" created successfully`, 'success');
                operationForm.reset();
                // Load operations list
                loadOperations();
            } else {
                showNotification(result.error || 'Failed to create operation', 'error');
            }
        } catch (error) {
            showNotification('Error creating operation: ' + error.message, 'error');
        } finally {
            submitButton.disabled = false;
            submitButton.textContent = 'Create Operation';
        }
    });
    
    // Setup operation control buttons
    document.getElementById('btn-start').addEventListener('click', () => handleOperationAction('start'));
    document.getElementById('btn-pause').addEventListener('click', () => handleOperationAction('pause'));
    document.getElementById('btn-resume').addEventListener('click', () => handleOperationAction('resume'));
    document.getElementById('btn-stop').addEventListener('click', () => handleOperationAction('stop', false));
    document.getElementById('btn-stop-graceful').addEventListener('click', () => handleOperationAction('stop', true));
    
    // Load active operation on startup
    loadActiveOperation();
    loadOperations();
    
    // Helper function for dashboard updates
    window.updateDashboard = function(data) {
        updateDashboardThrottled(data);
    };
    
    function updateDashboardThrottled(data) {
        // Update main metrics
        updateMetrics(data);
        
        // Update clients table with virtualization for large numbers
        updateClientsTable(data.clients);
        
        // Update results with pagination
        updateResultsTable(data.results);
        
        // Update timestamp
        updateLastUpdate(data.timestamp);
    }
});

function updateMetrics(data) {
    // Update main metrics with animation for changes
    const ppsElement = document.getElementById('total-pps');
    const hitsElement = document.getElementById('successful-hits');
    
    if (ppsElement) {
        const newPPS = data.total_pps.toFixed(2);
        if (ppsElement.textContent !== newPPS) {
            ppsElement.textContent = newPPS;
            ppsElement.classList.add('metric-updated');
            setTimeout(() => ppsElement.classList.remove('metric-updated'), 500);
        }
    }
    
    if (hitsElement) {
        const newHits = data.successful_hits.toString();
        if (hitsElement.textContent !== newHits) {
            hitsElement.textContent = newHits;
            hitsElement.classList.add('metric-updated');
            setTimeout(() => hitsElement.classList.remove('metric-updated'), 500);
        }
    }

    // Update system metrics if elements exist
    updateSystemMetrics(data.system_metrics);
    
    // Update task stats if elements exist
    updateTaskStats(data.task_stats);
}

function updateClientsTable(clients) {
    const clientsTbody = document.querySelector('#clients-table tbody');
    if (!clientsTbody) return;
    
    // Use document fragment for better performance
    const fragment = document.createDocumentFragment();
    
    if (clients && clients.length > 0) {
        // Sort clients by PPS descending for better visibility
        const sortedClients = [...clients].sort((a, b) => b.pps - a.pps);
        
        // Limit display to top 50 clients for performance
        const displayClients = sortedClients.slice(0, 50);
        
        displayClients.forEach(client => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td title="${client.id}">${client.id.substring(0, 8)}...</td>
                <td>${client.hostname || 'Unknown'}</td>
                <td class="pps-cell">${client.pps.toFixed(2)}</td>
                <td>${client.active_tasks || 0}</td>
                <td>${client.total_attempts || 0}</td>
                <td class="success-cell">${client.success_count || 0}</td>
                <td><span class="status-${client.status}">${client.status}</span></td>
            `;
            fragment.appendChild(row);
        });
        
        if (sortedClients.length > 50) {
            const row = document.createElement('tr');
            row.innerHTML = `<td colspan="7" style="text-align: center;">... and ${sortedClients.length - 50} more clients</td>`;
            fragment.appendChild(row);
        }
    } else {
        const row = document.createElement('tr');
        row.innerHTML = '<td colspan="7">No active clients</td>';
        fragment.appendChild(row);
    }
    
    clientsTbody.innerHTML = '';
    clientsTbody.appendChild(fragment);
}

function updateResultsTable(results) {
    const resultsTbody = document.querySelector('#results-table tbody');
    if (!resultsTbody) return;
    
    // Store all results for pagination
    if (results) {
        window.allResults = results;
    }
    
    const fragment = document.createDocumentFragment();
    
    if (window.allResults && window.allResults.length > 0) {
        // Calculate pagination
        const totalResults = window.allResults.length;
        const startIdx = (window.currentResultPage - 1) * window.RESULTS_PER_PAGE;
        const endIdx = Math.min(startIdx + window.RESULTS_PER_PAGE, totalResults);
        const pageResults = window.allResults.slice(startIdx, endIdx);
        
        pageResults.forEach(result => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${result.ip}:${result.port || 3389}</td>
                <td>${result.username}</td>
                <td title="${result.password}">${maskPassword(result.password)}</td>
                <td>${result.found_at || new Date().toLocaleTimeString()}</td>
                <td title="${result.client_id}">${(result.client_id || '').substring(0, 8)}...</td>
            `;
            fragment.appendChild(row);
        });
        
        // Add pagination info
        if (totalResults > window.RESULTS_PER_PAGE) {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td colspan="5" style="text-align: center;">
                    Showing ${startIdx + 1}-${endIdx} of ${totalResults} results
                    ${createPaginationControls()}
                </td>
            `;
            fragment.appendChild(row);
        }
    } else {
        const row = document.createElement('tr');
        row.innerHTML = '<td colspan="5">No results found yet</td>';
        fragment.appendChild(row);
    }
    
    resultsTbody.innerHTML = '';
    resultsTbody.appendChild(fragment);
}

function maskPassword(password) {
    if (!password || password.length <= 4) return password;
    return password.substring(0, 2) + '***' + password.substring(password.length - 2);
}

function createPaginationControls() {
    const totalPages = Math.ceil(window.allResults.length / window.RESULTS_PER_PAGE);
    let controls = '<div class="pagination">';
    
    if (window.currentResultPage > 1) {
        controls += `<button onclick="changePage(${window.currentResultPage - 1})">Previous</button>`;
    }
    
    controls += ` Page ${window.currentResultPage} of ${totalPages} `;
    
    if (window.currentResultPage < totalPages) {
        controls += `<button onclick="changePage(${window.currentResultPage + 1})">Next</button>`;
    }
    
    controls += '</div>';
    return controls;
}

// Define changePage globally for onclick handlers
window.changePage = function(page) {
    window.currentResultPage = page;
    updateResultsTable();
}

function updateSystemMetrics(metrics) {
    if (!metrics) return;
    
    const cpuElement = document.getElementById('cpu-usage');
    if (cpuElement) cpuElement.textContent = `${metrics.cpu_usage?.toFixed(1) || 0}%`;
    
    const memElement = document.getElementById('memory-usage');
    if (memElement) memElement.textContent = `${metrics.memory_percent?.toFixed(1) || 0}%`;
    
    const goroutinesElement = document.getElementById('goroutines');
    if (goroutinesElement) goroutinesElement.textContent = metrics.goroutines || 0;
    
    const connectionsElement = document.getElementById('connections');
    if (connectionsElement) connectionsElement.textContent = metrics.connections || 0;
}

function updateTaskStats(stats) {
    if (!stats) return;
    
    const totalTasksElement = document.getElementById('total-tasks');
    if (totalTasksElement) totalTasksElement.textContent = stats.total_tasks || 0;
    
    const activeTasksElement = document.getElementById('active-tasks');
    if (activeTasksElement) activeTasksElement.textContent = stats.active_tasks || 0;
    
    const completedTasksElement = document.getElementById('completed-tasks');
    if (completedTasksElement) completedTasksElement.textContent = stats.completed_tasks || 0;
    
    const successRateElement = document.getElementById('success-rate');
    if (successRateElement) successRateElement.textContent = `${(stats.success_rate || 0).toFixed(1)}%`;
}

function updateConnectionStatus(status) {
    const statusElement = document.getElementById('connection-status');
    if (statusElement) {
        statusElement.textContent = status;
        statusElement.className = `connection-${status}`;
    }
}

function updateLastUpdate(timestamp) {
    const element = document.getElementById('last-update');
    if (element) {
        const date = new Date(timestamp * 1000);
        element.textContent = date.toLocaleTimeString();
    }
}

function showNotification(message, type) {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.remove();
    }, 3000);
}

// Operation management functions
async function loadActiveOperation() {
    try {
        const response = await fetch('/api/operations/active');
        if (response.ok) {
            const operation = await response.json();
            updateActiveOperation(operation);
        }
    } catch (error) {
        console.error('Error loading active operation:', error);
    }
}

async function loadOperations() {
    try {
        const response = await fetch('/api/operations');
        if (response.ok) {
            const operations = await response.json();
            updateOperationsTable(operations);
        }
    } catch (error) {
        console.error('Error loading operations:', error);
    }
}

function updateActiveOperation(operation) {
    if (operation && operation.id) {
        activeOperationId = operation.id;
        operationState = operation.state;
        
        document.getElementById('active-operation-name').textContent = operation.name;
        document.getElementById('operation-state').textContent = operation.state;
        document.getElementById('operation-state').className = operation.state;
        
        // Update progress
        const progress = calculateOperationProgress(operation);
        document.getElementById('operation-progress').textContent = `${progress.toFixed(1)}%`;
        
        // Update button states
        updateOperationButtons(operation.state);
    } else {
        activeOperationId = null;
        operationState = 'idle';
        
        document.getElementById('active-operation-name').textContent = 'None';
        document.getElementById('operation-state').textContent = '--';
        document.getElementById('operation-state').className = '';
        document.getElementById('operation-progress').textContent = '--';
        
        // Disable all control buttons
        updateOperationButtons('none');
    }
}

function calculateOperationProgress(operation) {
    if (operation.total_targets === 0) return 0;
    return (operation.completed_targets / operation.total_targets) * 100;
}

function updateOperationButtons(state) {
    const btnStart = document.getElementById('btn-start');
    const btnPause = document.getElementById('btn-pause');
    const btnResume = document.getElementById('btn-resume');
    const btnStop = document.getElementById('btn-stop');
    const btnStopGraceful = document.getElementById('btn-stop-graceful');
    
    // Reset all buttons
    [btnStart, btnPause, btnResume, btnStop, btnStopGraceful].forEach(btn => btn.disabled = true);
    
    switch(state) {
        case 'idle':
            btnStart.disabled = false;
            break;
        case 'running':
            btnPause.disabled = false;
            btnStop.disabled = false;
            btnStopGraceful.disabled = false;
            break;
        case 'paused':
            btnResume.disabled = false;
            btnStop.disabled = false;
            break;
        case 'none':
            // All buttons stay disabled
            break;
    }
}

async function handleOperationAction(action, graceful = false) {
    if (!activeOperationId && action !== 'start') {
        showNotification('No active operation', 'error');
        return;
    }
    
    // For start action, we need to select an operation first
    if (action === 'start') {
        const operationId = await selectOperationToStart();
        if (!operationId) return;
        activeOperationId = operationId;
    }
    
    try {
        let endpoint = `/api/operations/${activeOperationId}/${action}`;
        const options = {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        };
        
        if (action === 'stop' && graceful) {
            options.body = JSON.stringify({ graceful: true });
        }
        
        const response = await fetch(endpoint, options);
        const result = await response.json();
        
        if (response.ok) {
            showNotification(`Operation ${action} successful`, 'success');
            // Reload active operation
            loadActiveOperation();
            loadOperations();
        } else {
            showNotification(result.error || `Failed to ${action} operation`, 'error');
        }
    } catch (error) {
        showNotification(`Error: ${error.message}`, 'error');
    }
}

async function selectOperationToStart() {
    try {
        const response = await fetch('/api/operations');
        if (!response.ok) {
            showNotification('Failed to load operations', 'error');
            return null;
        }
        
        const operations = await response.json();
        const idleOperations = operations.filter(op => op.state === 'idle');
        
        if (idleOperations.length === 0) {
            showNotification('No idle operations available to start', 'warning');
            return null;
        }
        
        // For simplicity, select the first idle operation
        // In a real app, you might want to show a selection dialog
        return idleOperations[0].id;
    } catch (error) {
        showNotification('Error selecting operation: ' + error.message, 'error');
        return null;
    }
}

function updateOperationsTable(operations) {
    const tbody = document.querySelector('#operations-table tbody');
    if (!tbody) return;
    
    const fragment = document.createDocumentFragment();
    
    if (operations && operations.length > 0) {
        operations.forEach(op => {
            const row = document.createElement('tr');
            const progress = calculateOperationProgress(op);
            
            row.innerHTML = `
                <td>${op.name}</td>
                <td><span class="${op.state}">${op.state}</span></td>
                <td>
                    <div class="progress-bar">
                        <div class="progress-bar-fill" style="width: ${progress}%"></div>
                        <div class="progress-text">${progress.toFixed(1)}%</div>
                    </div>
                </td>
                <td>${op.total_targets}</td>
                <td>${op.successful_hits}</td>
                <td>${new Date(op.created_at).toLocaleString()}</td>
                <td class="op-actions">
                    ${renderOperationActions(op)}
                </td>
            `;
            fragment.appendChild(row);
        });
    } else {
        const row = document.createElement('tr');
        row.innerHTML = '<td colspan="7">No operations found</td>';
        fragment.appendChild(row);
    }
    
    tbody.innerHTML = '';
    tbody.appendChild(fragment);
}

function renderOperationActions(operation) {
    let actions = '';
    
    if (operation.state === 'idle') {
        actions += `<button class="btn btn-sm btn-success" onclick="selectAndStartOperation('${operation.id}')">Start</button>`;
    }
    
    if (operation.state === 'completed' || operation.state === 'stopped') {
        actions += `<button class="btn btn-sm btn-danger" onclick="deleteOperation('${operation.id}')">Delete</button>`;
    }
    
    return actions;
}

// Global functions for button onclick handlers
window.selectAndStartOperation = async function(operationId) {
    activeOperationId = operationId;
    await handleOperationAction('start');
}

window.deleteOperation = async function(operationId) {
    if (!confirm('Are you sure you want to delete this operation?')) return;
    
    try {
        const response = await fetch(`/api/operations/${operationId}`, {
            method: 'DELETE',
        });
        
        if (response.ok) {
            showNotification('Operation deleted successfully', 'success');
            loadOperations();
        } else {
            const result = await response.json();
            showNotification(result.error || 'Failed to delete operation', 'error');
        }
    } catch (error) {
        showNotification('Error deleting operation: ' + error.message, 'error');
    }
}
