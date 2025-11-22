import { API } from './api.js';
import { renderUserDashboard } from './renderer.js';

/**
 * Loads the user dashboard (as a starting point for Battle and Inventory)
 */
export async function loadDashboard() {
	window.actionContainer.innerHTML = '<h2>Loading Dashboard...</h2>';
	try {
		const userInfo = await API.getUserInfo();
		window.currentLevel = userInfo.level; // Update global level
		window.actionContainer.innerHTML = renderUserDashboard(userInfo);
	} catch (error) {
		window.actionContainer.innerHTML = `<h2>Dashboard Error 🚨</h2><p>Failed to load user data.</p><p>Error: ${error.message}</p>`;
		console.error("Dashboard fetch error:", error);
	}
}