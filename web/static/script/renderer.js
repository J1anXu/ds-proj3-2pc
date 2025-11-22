// Function to generate common Pokémon card HTML
function generatePokemonCardHtml(pokemon, type) {
    let selectionElement = '';
    let lockStatus = pokemon.locked ? ' (LOCKED)' : '';

    if (type === 'inventory') {
        // pokemon._id.$oid is the ObjectID in MongoDB
        selectionElement = `<input type="checkbox" name="pokemon_id" value="${pokemon._id.$oid}" class="release-checkbox" style="position: absolute; top: 10px; left: 10px; width: 20px; height: 20px;" ${pokemon.locked ? 'disabled' : ''}>`;
    } else if (type === 'trade-create' || type === 'trade-fulfill') {
        // ToggleTradeSelection is a global function defined in main.js/window object
        const className = type === 'trade-create' ? 'create-trade-checkbox' : 'fulfill-trade-checkbox';

        selectionElement = `
            <div class="pokemon-card" style="display:inline-block; margin: 5px; position: relative; cursor: pointer;" data-id="${pokemon._id.$oid}" onclick="window.toggleTradeSelection(this, '${type === 'trade-create' ? 'create' : 'fulfill'}')">
                <input type="checkbox" class="trade-checkbox ${className}" value="${pokemon._id.$oid}" style="display: none;">
                <div class="selection-overlay">SELECTED</div>
                <h3>${pokemon.name}</h3>
                <img src="${pokemon.image}" alt="${pokemon.name}">
            </div>
        `;
    }
    
    // Base card structure
    const cardContent = `
        <h3>${pokemon.name}${lockStatus}</h3>
        <img src="${pokemon.image}" alt="${pokemon.name}">
        <ul class="stat-list">
            <li>HP: ${pokemon.hp}</li>
            <li>ATK: ${pokemon.atk}</li>
            <li>DEF: ${pokemon.def}</li>
        </ul>
    `;
    
    // Return different structure based on context
    if (type === 'trade-create' || type === 'trade-fulfill') {
        return selectionElement; // For trade, the selectionElement is the whole card wrapper
    } else {
        return `<div class="pokemon-card" style="display:inline-block; margin: 10px; position: relative;">${selectionElement}${cardContent}</div>`;
    }
}


export function renderGatchaResult(data) {
    const pokemon = data.new_pokemon;
    return `
        <div class="pokemon-card">
            <h2>🎉 Gatcha Pull! You Got: ${pokemon.name}! 🎉</h2>
            <img src="${pokemon.image}" alt="${pokemon.name}">
            <ul class="stat-list">
                <li><strong>HP:</strong> ${pokemon.hp}</li>
                <li><strong>ATK:</strong> ${pokemon.atk}</li>
                <li><strong>DEF:</strong> ${pokemon.def}</li>
            </ul>
            <p style="margin-top: 15px;">**This Pokémon has been added to your Inventory!**</p>
        </div>
    `;
}

export function renderInventory(pokemonList, handleRelease) {
    if (pokemonList.length === 0) {
        return `<h2>Inventory Empty</h2><p>You haven't caught any Pokemon yet! Go use the Gatcha!</p>`;
    }

    const inventoryHtml = pokemonList.map(pokemon => generatePokemonCardHtml(pokemon, 'inventory')).join('');

    return `
        <h2>Your Pokemon Inventory (${pokemonList.length} Caught)</h2>
        <form id="release-form" onsubmit="return false;"> 
            <p>Locked Pokémon (in trade) cannot be released.</p>
            <div style="display:flex; flex-wrap:wrap; justify-content:center;">${inventoryHtml}</div>
            <button type="button" onclick="window.handleRelease()" style="margin-top: 20px; padding: 10px 30px; background-color: #dc3545; color: white; border: none; border-radius: 5px; cursor: pointer;">
                Confirm Release Selected
            </button>
        </form>
    `;
}


export function renderBattle(data) {
    return `
        <h2>Battle Queue Status</h2>
        <p>Status: <strong>${data.status}</strong></p>
        <p>${data.message}</p>
        <p>Current Queue Position: <strong>#${data.position}</strong></p>
    `;
}

// --- TRADE RENDERERS ---

export function renderTradeMenu(inventory, pendingTrades, currentPlayer) {
    const inventoryString = JSON.stringify(inventory).replace(/"/g, '&quot;');
    
    const tradeListHtml = pendingTrades.map(trade => {
        const offeredNames = trade.offered_details.map(pokemon => pokemon.name).join(', ');
        return `
            <li class="trade-request-item" style="border: 1px solid #5bc0de; padding: 15px; margin-bottom: 10px; border-radius: 5px;">
                <p><strong>Offer ID:</strong> ${trade.id} | From: ${trade.creator}</p>
                <p><strong>Offering:</strong> ${offeredNames} (1 Total)</p>
                <p><strong>Requesting:</strong> 1 of YOUR Pokémon</p>
                <button class="menu-button" style="background-color: #28a745; margin-top: 10px;" onclick="window.renderFulfillTradeForm('${trade.id}', 1, '${trade.creator}')">Fulfill This Trade</button>
            </li>
        `;
    }).join('');

    return `
        <div style="width: 100%;">
            <h1>Pokémon Trade Center</h1>
            
            <div style="display: flex; justify-content: space-around; margin-bottom: 30px;">
                <button class="menu-button" style="background-color: #28a745;" onclick="window.renderCreateTradeForm(${inventoryString})">
                    + Create New Trade
                </button>
            </div>
            
            <h2>Pending Trade Requests (${pendingTrades.length})</h2>
            <ul style="list-style: none; padding: 0; max-width: 600px; margin: 0 auto; text-align: left;">
                ${tradeListHtml || '<p>No pending trade requests at the moment.</p>'}
            </ul>
        </div>
    `;
}

export function renderCreateTradeFormHTML(inventory) {
    const inventoryHtml = inventory.map(pokemon => generatePokemonCardHtml(pokemon, 'trade-create')).join('');

    return `
        <h1>Create New Trade (Select 1 to Offer)</h1>
        <div id="selection-status">Selected: 0</div>
        
        <div style="display:flex; flex-wrap:wrap; justify-content:center;">${inventoryHtml}</div>
        
        <div style="margin-top: 20px;">
            <button onclick="window.handleCreateTrade()" class="menu-button" style="background-color: #28a745;">Confirm Trade Offer</button>
            <button onclick="window.loadContent('trade')" class="menu-button" style="background-color: #6c757d;">Cancel</button>
        </div>
    `;
}

export function renderFulfillTradeFormHTML(tradeId, requestedCount, creator, inventory) {
    const availableInventory = inventory.filter(pokemon => !pokemon.locked);
    
    // We pass 1 to generatePokemonCardHtml for max selection status
    const inventoryWithCount = availableInventory.map(pokemon => ({...pokemon, requiredCount: 1}));

    const inventoryHtml = inventoryWithCount.map(pokemon => generatePokemonCardHtml(pokemon, 'trade-fulfill')).join('');

    return `
        <h1>Fulfill Trade Request from ${creator}</h1>
        <h2>You must select exactly ONE Pokémon to complete this trade.</h2>
        <div id="selection-status">Selected: 0 / 1</div>

        <p>Select your 1 Pokémon to send:</p>
        <div style="display:flex; flex-wrap:wrap; justify-content:center;">${inventoryHtml || '<p>Your available inventory is empty!</p>'}</div>
        
        <div style="margin-top: 20px;">
            <button onclick="window.handleFulfillTrade('${tradeId}', 1)" class="menu-button" style="background-color: #28a745;" id="fulfill-confirm-btn" disabled>Confirm Fulfillment</button>
            <button onclick="window.loadContent('trade')" class="menu-button" style="background-color: #6c757d;">Cancel</button>
        </div>
    `;
}

export function renderUserDashboard(userInfo) {
    const xpNeeded = userInfo.xp_to_next_level;
    const currentLevel = userInfo.level;
    const currentXp = userInfo.xp;

    // Calculate XP percentage for a simple bar
    const xpPercent = Math.min(100, (currentXp / (currentXp + xpNeeded)) * 100);

    return `
        <h2>${userInfo.email} Dashboard</h2>
        <div style="background: #34495e; padding: 20px; border-radius: 8px;">
            <p style="font-size: 1.5em; margin: 0;">Level: <strong>${currentLevel}</strong></p>
            <p>Wins: ${userInfo.wins} | Losses: ${userInfo.losses}</p>
            
            <div style="margin-top: 15px;">
                <p>XP Progress: ${currentXp} / ${currentXp + xpNeeded}</p>
                <div style="background: #ecf0f1; border-radius: 10px; height: 15px; overflow: hidden;">
                    <div style="width: ${xpPercent}%; height: 100%; background: #f1c40f; transition: width 0.5s;"></div>
                </div>
            </div>
            <p style="font-size: 0.9em; margin-top: 10px;">XP to next level: ${xpNeeded}</p>
        </div>
    `;
}

export function renderBattleQueue(status, position) {
    let message = '';
    
    if (status === 'queue') {
        message = `<h2>Searching for Opponent... ⏱️</h2>
                   <p>You are currently in the queue.</p>
                   <p>Your current position: <strong>#${position}</strong></p>
                   <p>Do not navigate away! We will notify you when a match is found.</p>`;
    } else {
        message = '<h2>Battle Ready!</h2><p>Checking for results...</p>';
    }

    return message;
}

export function renderBattleResult(data) {
    const result = data.result;
    const isWin = result.result_message === 'WIN!';
    const color = isWin ? '#28a745' : '#dc3545';
    
    let levelUpMessage = '';
    if (result.player_level_up > 0) {
        // Use window.currentLevel which should have been updated after the battle XP calculation
        levelUpMessage = `<h3 style="color: #f1c40f;">LEVEL UP! You reached Level ${window.currentLevel + result.player_level_up}! 🎉</h3>`;
    }

    return `
        <div style="text-align: center; max-width: 600px; margin: 0 auto;">
            <h1 style="color: ${color}; border-bottom: 3px solid ${color}; padding-bottom: 15px;">
                BATTLE COMPLETE: ${result.result_message}
            </h1>
            
            ${levelUpMessage}

            <div style="display: flex; justify-content: space-around; margin-top: 20px;">
                <p>Your Pokémon: <strong>${result.player_mon}</strong></p>
                <p>Opponent's Pokémon: <strong>${result.opponent_mon}</strong></p>
            </div>

            <div style="background: #34495e; padding: 20px; border-radius: 8px; margin-top: 20px;">
                <p style="font-size: 1.2em;">XP Gained: <strong>+${result.player_xp_gain}</strong></p>
            </div>
            
            <button onclick="window.handleBattleEndConfirmation()" class="menu-button" style="margin-top: 30px; background-color: #3498db;">OK</button>
        </div>
    `;
}