const BASE_URL = 'http://localhost:5000/api/';

/**
 * Handles all generic GET/POST API calls.
 * @param {string} endpoint - The API endpoint (e.g., 'inventory', 'gatcha', etc...).
 * @param {string} method - The HTTP method ('GET', 'POST', 'PUT', 'DELETE').
 * @param {Object} bodyData - Data to send with a request.
 * @returns {Promise<Object>} - The JSON response data.
 */
async function callApi(endpoint, method, bodyData = null) {
    const url = BASE_URL + endpoint;
    const fetchOptions = {
        method: method,
        headers: {
            'Content-Type': 'application/json'
        }
    };

    if (bodyData) {
        fetchOptions.body = JSON.stringify(bodyData);
    }

    const response = await fetch(url, fetchOptions);
    
    if (!response.ok) {
        // Fetch the error message from the server if available
        const errorData = await response.json();
        throw new Error(errorData.message || `HTTP error! status: ${response.status}`);
    }
    
    return response.json();
}

// Specific API functions
export const API = {
    // Inventory related APIs
    getInventory: () => callApi('inventory', 'GET'),
    releasePokemon: (ids) => callApi('release', 'DELETE', { ids }),

    // Catch Pokemon API
    runGatcha: () => callApi('gatcha', 'POST'),

    // Trade related APIs
    getTradeData: () => callApi('trade', 'GET'),
    createTrade: (offering_ids, looking_for_count) => 
        callApi('trade/create', 'POST', { offering_ids, looking_for_count }), 
    fulfillTrade: (trade_id, fulfilling_ids) => 
        callApi('trade/fulfill', 'PUT', { trade_id, fulfilling_ids }),

    // User related APIs
    getUserInfo: () => callApi('user/info', "GET"), 
    
    // Battle related APIs
    getBattleData: () => callApi('battle', 'GET'),
    enterQueue: () => callApi('battle/queue', 'POST')
};