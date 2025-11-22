import { API } from './api.js';
import { renderInventory } from './renderer.js';

/**
 * Loads the main inventory view by fetching data and rendering it.
 */
export async function loadInventoryView() {
    window.actionContainer.innerHTML = '<h2>Loading Inventory...</h2>';
    try {
        const data = await API.getInventory();
        window.actionContainer.innerHTML = renderInventory(data);
    } catch (error) {
        window.actionContainer.innerHTML = `<h2>Inventory Error 🚨</h2><p>Failed to load inventory.</p><p>Error: ${error.message}</p>`;
        console.error("Inventory fetch error:", error);
    }
}

/**
 * Handles the release API call and subsequent reload.
 */
export async function handleRelease() {
    const checkboxes = document.querySelectorAll('#release-form input[name="pokemon_id"]:checked');
    const idsToRelease = Array.from(checkboxes).map(cb => cb.value);

    if (idsToRelease.length === 0) {
        alert("Please select at least one Pokémon to release.");
        return;
    }

    if (!confirm(`Are you sure you want to release ${idsToRelease.length} Pokémon? This cannot be undone!`)) {
        return;
    }

    window.actionContainer.innerHTML = `<h2>Releasing ${idsToRelease.length} Pokémon...</h2>`;

    try {
        const data = await API.releasePokemon(idsToRelease);
        alert(data.message);
        loadInventoryView(); // Reload inventory view after successful release
    } catch (error) {
        window.actionContainer.innerHTML = `<h2>Release Failed 😞</h2><p>Error: ${error.message}</p>`;
        console.error("Release error:", error);
    }
}