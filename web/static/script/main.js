// The orchestrator script that ties together various modules for the game client

import { renderBattle, renderGatchaResult } from './renderer.js';
import { loadInventoryView, handleRelease } from './inventory.js';
import { loadTradeMenu, renderCreateTradeForm, renderFulfillTradeForm, handleCreateTrade, handleFulfillTrade, toggleTradeSelection } from './trade.js';
import { enterMatchmakingQueue, handleBattleEndConfirmation, setupSocketListeners } from './battle.js'; // NEW
import { loadDashboard } from './user.js'; // NEW
import { API } from './api.js'; // Ensure api.js is imported for generic calls

const actionContainer = document.getElementById('action-container');
window.actionContainer = actionContainer; // Make available globally for modules

// --- Global Functions (Attached to window for inline HTML/module calls) ---

// Expose these domain-specific functions globally for HTML onclick handlers
window.handleRelease = handleRelease;
window.loadTradeMenu = loadTradeMenu;
window.renderCreateTradeForm = renderCreateTradeForm;
window.renderFulfillTradeForm = renderFulfillTradeForm;
window.handleCreateTrade = handleCreateTrade;
window.handleFulfillTrade = handleFulfillTrade;
window.toggleTradeSelection = toggleTradeSelection;
window.loadDashboard = loadDashboard;
window.handleBattleEndConfirmation = handleBattleEndConfirmation;

// Global Socket.IO instance and state
// Will store the socket connection
window.socket = null; 
// Store battle state globally
window.battleState = {
    inQueue: false,
    battleId: null,
    queueInterval: null,
    currentLevel: 1 // Placeholder for display
};

/**
 * Main function to load content based on the button clicked.
 * It now delegates domain logic to the respective module files.
 * @param {string} endpoint - The data-endpoint attribute from the button.
 */
window.loadContent = async function(endpoint) {
    
    actionContainer.innerHTML = '<h2>Loading...</h2>';
    
    try {
        let content = '';

        // Clear any existing queue intervals when changing screens
        if(window.battleState.queueInterval) {
            clearInterval(window.battleState.queueInterval);
            window.battleState.queueInterval = null;
        }

        if (endpoint === 'battle') {
            return enterMatchmakingQueue();
        }
        
        // Check if the endpoint requires dashboard loading (e.g., initially or after non-battle actions)
        if (endpoint === 'inventory' || endpoint === 'release') {
            // Load inventory view which should include a user dashboard above it
            return loadInventoryView();
        }
        
        if (endpoint === 'trade') {
            // Trade logic is handled by trade.js
            return loadTradeMenu();
        }

        if (endpoint === 'gatcha') {
            // Gatcha is a simple API call and render, kept here for flow
            const data = await API.runGatcha();
            content = renderGatchaResult(data);
        } else if (endpoint === 'battle') {
            const data = await API.getBattleData();
            content = renderBattle(data);
        } else {
             content = '<h2>Action Not Implemented</h2>';
        }

        actionContainer.innerHTML = content;
        
    } catch (error) {
        actionContainer.innerHTML = `<h2>Server Error 🚨</h2><p>Could not connect to the server or process the request.</p><p>Error: ${error.message}</p>`;
        console.error("Fetch error:", error);
    }
}

// --- Initialization ---
function init() {
    // Initialize Socket.IO connection with automatic reconnection
    const socket = io('http://localhost:5000', {
        path: '/socket.io',  // Remove trailing slash
        transports: ['websocket', 'polling'],  // Allow fallback to polling
        reconnection: true,
        reconnectionDelay: 1000,
        reconnectionDelayMax: 5000,
        reconnectionAttempts: 5,
        forceNew: true,
        timeout: 10000
    });

    window.socket = socket; 

    // Pass the socket instance to the battle module for event binding
    setupSocketListeners(window.socket);

    // Set up button listeners and load dashboard
    const dynamicButtons = document.querySelectorAll('.menu-container button');
    dynamicButtons.forEach(button => {
        button.addEventListener('click', () => {
            const endpoint = button.getAttribute('data-endpoint');
            window.loadContent(endpoint); 
        });
    });

    // Load the dashboard when the page first loads
    loadDashboard();
}

document.addEventListener('DOMContentLoaded', init);