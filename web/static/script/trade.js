import { API } from './api.js';
import { renderTradeMenu, renderCreateTradeFormHTML, renderFulfillTradeFormHTML } from './renderer.js';

/**
 * Loads the main trade menu view.
 */
export async function loadTradeMenu() {
    window.actionContainer.innerHTML = '<h2>Loading Trade Data...</h2>';
    
    try {
        const data = await API.getTradeData();
        window.actionContainer.innerHTML = renderTradeMenu(
            data.inventory, 
            data.pending_trades, 
            data.current_player
        );
    } catch (error) {
        window.actionContainer.innerHTML = `<h2>Trade Server Error 🚨</h2><p>Could not load trade data.</p><p>Error: ${error.message}</p>`;
        console.error("Trade fetch error:", error);
    }
}

/**
 * Renders the form for creating a new trade offer.
 * @param {Array<Object>} inventory - The player's available Pokémon.
 */
export function renderCreateTradeForm(inventory) {
    window.actionContainer.innerHTML = renderCreateTradeFormHTML(inventory, 1);
}

/**
 * Renders the form for fulfilling an existing trade request.
 */
export async function renderFulfillTradeForm(tradeId, requestedCount, creator) {
    // requestedCount will always be 1, but we keep it for now as a parameter for consistency
    window.actionContainer.innerHTML = '<h2>Loading Inventory...</h2>';
    try {
        const inventory = await API.getInventory();
        window.actionContainer.innerHTML = renderFulfillTradeFormHTML(tradeId, 1, creator, inventory);
    } catch (error) {
        window.actionContainer.innerHTML = `<h2>Error loading inventory for trade fulfillment.</h2><p>${error.message}</p>`;
    }
}


// --- Action Handlers ---

/**
 * Handles the API call to create a new trade.
 */
export async function handleCreateTrade() {
    const checkboxes = document.querySelectorAll('.create-trade-checkbox:checked');
    const offeringIds = Array.from(checkboxes).map(cb => cb.value);
    
    // Validation is simplified: MUST select 1
    if (offeringIds.length !== 1) {
        alert("You must select exactly ONE Pokémon to offer for a 1-for-1 trade.");
        return;
    }

    window.actionContainer.innerHTML = `<h2>Sending 1-for-1 Trade Request...</h2>`;

    try {
        // looking_for_count is hardcoded to 1 on the server, but we send 1 to be safe
        const data = await API.createTrade(offeringIds, 1); 
        alert(data.message);
        loadTradeMenu();
    } catch (error) {
        window.actionContainer.innerHTML = `<h2>Trade Creation Failed 😞</h2><p>Error: ${error.message}</p>`;
        console.error("Trade creation error:", error);
    }
}

/**
 * Handles the API call to fulfill an existing trade.
 */
export async function handleFulfillTrade(tradeId, requiredCount) {
    const checkboxes = document.querySelectorAll('.fulfill-trade-checkbox:checked');
    const fulfillingIds = Array.from(checkboxes).map(cb => cb.value);
    
    // Validation is simplified: MUST select 1
    if (fulfillingIds.length !== 1) {
        alert(`You must select exactly ONE Pokémon to fulfill this 1-for-1 trade.`);
        return;
    }

    window.actionContainer.innerHTML = `<h2>Fulfilling 1-for-1 Trade...</h2>`;

    try {
        const data = await API.fulfillTrade(tradeId, fulfillingIds);
        alert(data.message);
        loadTradeMenu();
    } catch (error) {
        window.actionContainer.innerHTML = `<h2>Trade Fulfillment Failed 😞</h2><p>Error: ${error.message}</p>`;
        console.error("Trade fulfillment error:", error);
    }
}

/**
 * Logic for selecting/unselecting Pokémon in the trade forms.
 */
export function toggleTradeSelection(element, type) {
    const checkbox = element.querySelector('.trade-checkbox');
    const overlay = element.querySelector('.selection-overlay');
    let isCurrentlySelected = checkbox.checked;

    checkbox.checked = !isCurrentlySelected;
    overlay.style.display = checkbox.checked ? 'flex' : 'none';

    const finalSelected = document.querySelectorAll(`.${type}-trade-checkbox:checked`);
    
    const statusElement = document.getElementById('selection-status');
    if (statusElement) {
        statusElement.textContent = `Selected: ${finalSelected.length} / 1`;
    }
    
    // Only enable fulfill button if exactly 1 is selected
    if (type === 'fulfill') {
        const btn = document.getElementById('fulfill-confirm-btn');
        if (btn) btn.disabled = (finalSelected.length !== 1);
    }
}