import { renderBattleQueue, renderBattleResult } from './renderer.js';
import { loadDashboard } from './user.js'; 

// Store battle state globally on the window object (initialized in main.js)

// --- Socket Listener Setup ---
export function setupSocketListeners(socket) {
    
    // Handle queue status update (sent by server if no match is found)
    socket.on('queue_update', (data) => {
        window.battleState.inQueue = true;
        window.actionContainer.innerHTML = renderBattleQueue('queue', data.position);
    });

    // Listen for the direct 'battle_result' event from the server
    socket.on('battle_result', (data) => {
        window.battleState.inQueue = false;
        
        // Pass the already-fetched data directly to the renderer
        showBattleResult(data);
    });

    // Handle server-side errors
    socket.on('queue_error', (data) => {
        alert(`Queue Error: ${data.message}`);
        window.battleState.inQueue = false;
        loadDashboard();
    });
}

/**
 * Enters the player into the matchmaking queue by emitting an event.
 */
export async function enterMatchmakingQueue() {
    
    if (window.battleState.inQueue) {
        // Show current queue screen
        window.actionContainer.innerHTML = renderBattleQueue('queue', '...'); 
        return;
    }

    window.actionContainer.innerHTML = renderBattleQueue('loading');
    
    // Emit the event to the server instead of making an HTTP request
    if (window.socket && window.socket.connected) {
        window.socket.emit('join_queue');
        // Assume success until the server says otherwise
        window.battleState.inQueue = true; 
    } else {
        alert("Socket connection failed. Cannot join queue.");
        loadDashboard();
    }
}

// Exported function to be called by the "OK" button in the rendered HTML
export function handleBattleEndConfirmation() {
    // This is where you put any final cleanup logic before returning to the dashboard
    loadDashboard();
}

/**
 * Displays the final battle result (data is now received directly via SocketIO).
 */
function showBattleResult(result) {
    window.actionContainer.innerHTML = '<h2>Battle Ready! Checking for results...</h2>';
    
    // Reset battle state
    window.battleState.inQueue = false;

    if (result.status === 'complete') {
        // The user has to click 'OK' before the dashboard loads
        window.actionContainer.innerHTML = renderBattleResult(result);
    } else {
        window.actionContainer.innerHTML = `<h2>Result Error 🚨</h2><p>Unexpected result status: ${result.message}</p>`;
        console.error("Result retrieval error:", result.message);
        loadDashboard();
    }
}