/**
 * Caps Edge - Frontend JavaScript
 * Handles data fetching, table rendering, and sorting
 */

// State
let players = [];
let currentSort = { field: 'points', direction: 'desc' };

// DOM Elements
const loadingEl = document.getElementById('loading');
const errorEl = document.getElementById('error');
const tableContainer = document.getElementById('table-container');
const playersBody = document.getElementById('players-body');
const lastUpdatedEl = document.getElementById('last-updated');
const legendEl = document.getElementById('legend');

/**
 * Format relative time (e.g., "2 hours ago")
 */
function formatRelativeTime(dateString) {
    if (!dateString) return 'Never';

    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `Updated ${diffMins} minute${diffMins === 1 ? '' : 's'} ago`;
    if (diffHours < 24) return `Updated ${diffHours} hour${diffHours === 1 ? '' : 's'} ago`;
    return `Updated ${diffDays} day${diffDays === 1 ? '' : 's'} ago`;
}

/**
 * Get CSS class for percentile coloring
 */
function getPercentileClass(percentile) {
    if (percentile === null || percentile === undefined) return '';
    if (percentile >= 75) return 'percentile-high';
    if (percentile < 25) return 'percentile-low';
    return '';
}

/**
 * Format a stat value with percentile
 */
function formatStatWithPercentile(value, percentile, decimals = 1) {
    if (value === null || value === undefined) return '-';

    const formattedValue = typeof value === 'number' ? value.toFixed(decimals) : value;
    const percentileClass = getPercentileClass(percentile);
    const percentileText = percentile !== null && percentile !== undefined ? `(${percentile})` : '';

    return `<span class="${percentileClass}">${formattedValue} <span class="text-gray-500 text-xs">${percentileText}</span></span>`;
}

/**
 * Format a plain stat value
 */
function formatStat(value, decimals = null) {
    if (value === null || value === undefined) return '-';
    if (decimals !== null && typeof value === 'number') {
        return value.toFixed(decimals);
    }
    return value;
}

/**
 * Generate Hockey-Reference search URL
 */
function getHockeyRefUrl(playerName) {
    return `https://www.hockey-reference.com/search/search.fcgi?search=${encodeURIComponent(playerName)}`;
}

/**
 * Render a single player row
 */
function renderPlayerRow(player) {
    const stats = player.stats || {};
    const edge = player.edge_stats || {};

    // Position display - convert single letter to full for readability
    const positionMap = { 'C': 'C', 'L': 'LW', 'R': 'RW', 'D': 'D' };
    const position = positionMap[player.position] || player.position;

    // FO% - show dash for non-centers
    const showFaceoff = player.position === 'C';

    return `
        <tr class="table-row border-b border-dark-border hover:bg-dark-card/50">
            <td class="table-cell sticky left-0 bg-dark-bg">${player.jersey_number || '-'}</td>
            <td class="table-cell sticky left-12 bg-dark-bg">
                <a href="${getHockeyRefUrl(player.name)}"
                   target="_blank"
                   rel="noopener"
                   class="text-caps-red hover:underline">
                    ${player.name}
                </a>
            </td>
            <td class="table-cell">${position}</td>
            <td class="table-cell">${formatStat(stats.games_played)}</td>
            <td class="table-cell">${formatStat(stats.avg_toi, 1)}</td>
            <td class="table-cell">${formatStat(stats.goals)}</td>
            <td class="table-cell">${formatStat(stats.assists)}</td>
            <td class="table-cell font-semibold">${formatStat(stats.points)}</td>
            <td class="table-cell ${stats.plus_minus > 0 ? 'text-green-400' : stats.plus_minus < 0 ? 'text-red-400' : ''}">${stats.plus_minus > 0 ? '+' : ''}${formatStat(stats.plus_minus)}</td>
            <td class="table-cell">${formatStat(stats.hits)}</td>
            <td class="table-cell">${formatStat(stats.pim)}</td>
            <td class="table-cell">${showFaceoff ? formatStat(stats.faceoff_win_pct, 1) : '-'}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.top_speed_mph, edge.top_speed_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.bursts_20_plus, edge.bursts_20_percentile, 0)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.bursts_22_plus, edge.bursts_22_percentile, 0)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.distance_per_game_miles, edge.distance_percentile, 2)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.off_zone_time_pct, edge.off_zone_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.def_zone_time_pct, edge.def_zone_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.zone_starts_off_pct, edge.zone_starts_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.top_shot_speed_mph, edge.shot_speed_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.hustle_score, edge.hustle_percentile)}</td>
        </tr>
    `;
}

/**
 * Get nested value from object using dot notation
 */
function getNestedValue(obj, path) {
    const parts = path.split('.');
    let value = obj;

    for (const part of parts) {
        if (value === null || value === undefined) return null;
        value = value[part];
    }

    return value;
}

/**
 * Sort players by field
 */
function sortPlayers(field) {
    // Toggle direction if same field
    if (currentSort.field === field) {
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort.field = field;
        // Default to descending for most stats, ascending for name
        currentSort.direction = field === 'name' ? 'asc' : 'desc';
    }

    // Map field names to data paths
    const fieldMap = {
        'jersey_number': 'jersey_number',
        'name': 'name',
        'position': 'position',
        'games_played': 'stats.games_played',
        'avg_toi': 'stats.avg_toi',
        'goals': 'stats.goals',
        'assists': 'stats.assists',
        'points': 'stats.points',
        'plus_minus': 'stats.plus_minus',
        'hits': 'stats.hits',
        'pim': 'stats.pim',
        'faceoff_win_pct': 'stats.faceoff_win_pct',
        'top_speed_mph': 'edge_stats.top_speed_mph',
        'bursts_20_plus': 'edge_stats.bursts_20_plus',
        'bursts_22_plus': 'edge_stats.bursts_22_plus',
        'distance_per_game_miles': 'edge_stats.distance_per_game_miles',
        'off_zone_time_pct': 'edge_stats.off_zone_time_pct',
        'def_zone_time_pct': 'edge_stats.def_zone_time_pct',
        'zone_starts_off_pct': 'edge_stats.zone_starts_off_pct',
        'top_shot_speed_mph': 'edge_stats.top_shot_speed_mph',
        'hustle_score': 'edge_stats.hustle_score'
    };

    const path = fieldMap[field] || field;

    players.sort((a, b) => {
        let aVal = getNestedValue(a, path);
        let bVal = getNestedValue(b, path);

        // Handle nulls - push to end
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        // String comparison for name
        if (typeof aVal === 'string') {
            const comparison = aVal.localeCompare(bVal);
            return currentSort.direction === 'asc' ? comparison : -comparison;
        }

        // Numeric comparison
        const comparison = aVal - bVal;
        return currentSort.direction === 'asc' ? comparison : -comparison;
    });

    renderTable();
    updateSortIndicators();
}

/**
 * Update sort indicator arrows in headers
 */
function updateSortIndicators() {
    // Remove all existing indicators
    document.querySelectorAll('.sort-indicator').forEach(el => el.remove());

    // Add indicator to current sort column
    const header = document.querySelector(`th[data-sort="${currentSort.field}"]`);
    if (header) {
        const indicator = document.createElement('span');
        indicator.className = 'sort-indicator ml-1';
        indicator.textContent = currentSort.direction === 'asc' ? '▲' : '▼';
        header.appendChild(indicator);
    }
}

/**
 * Render the players table
 */
function renderTable() {
    playersBody.innerHTML = players.map(renderPlayerRow).join('');
}

/**
 * Setup sort click handlers
 */
function setupSortHandlers() {
    document.querySelectorAll('th[data-sort]').forEach(header => {
        header.addEventListener('click', () => {
            sortPlayers(header.dataset.sort);
        });
    });
}

/**
 * Fetch players data from API
 */
async function fetchPlayers() {
    try {
        const response = await fetch('/api/players');
        if (!response.ok) {
            throw new Error('Failed to fetch players');
        }

        const data = await response.json();
        players = data.players;

        // Update last updated time
        lastUpdatedEl.textContent = formatRelativeTime(data.last_updated);

        // Sort by points by default
        sortPlayers('points');

        // Show table, hide loading
        loadingEl.classList.add('hidden');
        tableContainer.classList.remove('hidden');
        legendEl.classList.remove('hidden');

    } catch (error) {
        console.error('Error fetching players:', error);
        loadingEl.classList.add('hidden');
        errorEl.classList.remove('hidden');
    }
}

/**
 * Initialize the app
 */
function init() {
    setupSortHandlers();
    fetchPlayers();

    // Refresh data periodically (every 5 minutes)
    setInterval(fetchPlayers, 5 * 60 * 1000);
}

// Start the app when DOM is ready
document.addEventListener('DOMContentLoaded', init);
