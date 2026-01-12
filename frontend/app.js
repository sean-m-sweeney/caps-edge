/**
 * Caps Edge - Frontend JavaScript
 * Handles data fetching, table rendering, and sorting
 */

// State
let allPlayers = [];
let forwards = [];
let defensemen = [];
let sortState = {
    forwards: { field: 'points', direction: 'desc' },
    defensemen: { field: 'points', direction: 'desc' }
};

// DOM Elements
const loadingEl = document.getElementById('loading');
const errorEl = document.getElementById('error');
const tableContainer = document.getElementById('table-container');
const forwardsBody = document.getElementById('forwards-body');
const defensemenBody = document.getElementById('defensemen-body');
const lastUpdatedEl = document.getElementById('last-updated');
const legendEl = document.getElementById('legend');
const infoSection = document.getElementById('info-section');
const infoToggle = document.getElementById('info-toggle');
const infoContent = document.getElementById('info-content');

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
            <td class="table-cell sticky left-0 bg-dark-bg w-10">${player.jersey_number || '-'}</td>
            <td class="table-cell min-w-[150px]">
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
            <td class="table-cell">${formatStatWithPercentile(stats.shots_per_60, edge.shots_percentile)}</td>
            <td class="table-cell">${showFaceoff ? formatStat(stats.faceoff_win_pct, 1) : '-'}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.top_speed_mph, edge.top_speed_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.bursts_20_plus, edge.bursts_20_percentile, 0)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.bursts_22_plus, edge.bursts_22_percentile, 0)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.distance_per_game_miles, edge.distance_percentile, 2)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.off_zone_time_pct, edge.off_zone_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.def_zone_time_pct, edge.def_zone_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.zone_starts_off_pct, edge.zone_starts_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.top_shot_speed_mph, edge.shot_speed_percentile)}</td>
            <td class="table-cell">${formatStatWithPercentile(edge.motor_index, edge.motor_percentile)}</td>
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
 * Sort players array by field
 */
function sortPlayersArray(playersArray, field, direction) {
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
        'shots_per_60': 'stats.shots_per_60',
        'faceoff_win_pct': 'stats.faceoff_win_pct',
        'top_speed_mph': 'edge_stats.top_speed_mph',
        'bursts_20_plus': 'edge_stats.bursts_20_plus',
        'bursts_22_plus': 'edge_stats.bursts_22_plus',
        'distance_per_game_miles': 'edge_stats.distance_per_game_miles',
        'off_zone_time_pct': 'edge_stats.off_zone_time_pct',
        'def_zone_time_pct': 'edge_stats.def_zone_time_pct',
        'zone_starts_off_pct': 'edge_stats.zone_starts_off_pct',
        'top_shot_speed_mph': 'edge_stats.top_shot_speed_mph',
        'motor_index': 'edge_stats.motor_index'
    };

    const path = fieldMap[field] || field;

    playersArray.sort((a, b) => {
        let aVal = getNestedValue(a, path);
        let bVal = getNestedValue(b, path);

        // Handle nulls - push to end
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        // String comparison for name
        if (typeof aVal === 'string') {
            const comparison = aVal.localeCompare(bVal);
            return direction === 'asc' ? comparison : -comparison;
        }

        // Numeric comparison
        const comparison = aVal - bVal;
        return direction === 'asc' ? comparison : -comparison;
    });
}

/**
 * Sort a specific table
 */
function sortTable(tableType, field) {
    const state = sortState[tableType];

    // Toggle direction if same field
    if (state.field === field) {
        state.direction = state.direction === 'asc' ? 'desc' : 'asc';
    } else {
        state.field = field;
        // Default to descending for most stats, ascending for name
        state.direction = field === 'name' ? 'asc' : 'desc';
    }

    if (tableType === 'forwards') {
        sortPlayersArray(forwards, field, state.direction);
        renderForwards();
    } else {
        sortPlayersArray(defensemen, field, state.direction);
        renderDefensemen();
    }

    updateSortIndicators(tableType);
}

/**
 * Update sort indicator arrows in headers for a specific table
 */
function updateSortIndicators(tableType) {
    const tableId = tableType === 'forwards' ? 'forwards-table' : 'defensemen-table';
    const table = document.getElementById(tableId);
    const state = sortState[tableType];

    // Remove all existing indicators in this table
    table.querySelectorAll('.sort-indicator').forEach(el => el.remove());

    // Add indicator to current sort column
    const header = table.querySelector(`th[data-sort="${state.field}"]`);
    if (header) {
        const indicator = document.createElement('span');
        indicator.className = 'sort-indicator ml-1';
        indicator.textContent = state.direction === 'asc' ? '▲' : '▼';
        header.appendChild(indicator);
    }
}

/**
 * Render the forwards table
 */
function renderForwards() {
    forwardsBody.innerHTML = forwards.map(renderPlayerRow).join('');
}

/**
 * Render the defensemen table
 */
function renderDefensemen() {
    defensemenBody.innerHTML = defensemen.map(renderPlayerRow).join('');
}

/**
 * Setup sort click handlers
 */
function setupSortHandlers() {
    document.querySelectorAll('th[data-sort]').forEach(header => {
        header.addEventListener('click', () => {
            const tableType = header.dataset.table;
            const field = header.dataset.sort;
            sortTable(tableType, field);
        });
    });
}

/**
 * Setup info section toggle
 */
function setupInfoToggle() {
    if (infoToggle && infoContent) {
        infoToggle.addEventListener('click', function() {
            const isExpanded = infoContent.classList.toggle('expanded');
            this.textContent = isExpanded ? 'How to read this data ▲' : 'How to read this data ▼';
        });
    }
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
        allPlayers = data.players;

        // Split into forwards and defensemen
        forwards = allPlayers.filter(p => ['C', 'L', 'R'].includes(p.position));
        defensemen = allPlayers.filter(p => p.position === 'D');

        // Update last updated time
        lastUpdatedEl.textContent = formatRelativeTime(data.last_updated);

        // Sort by points by default
        sortPlayersArray(forwards, 'points', 'desc');
        sortPlayersArray(defensemen, 'points', 'desc');

        // Render both tables
        renderForwards();
        renderDefensemen();

        // Update sort indicators
        updateSortIndicators('forwards');
        updateSortIndicators('defensemen');

        // Show table, hide loading
        loadingEl.classList.add('hidden');
        tableContainer.classList.remove('hidden');
        legendEl.classList.remove('hidden');
        infoSection.classList.remove('hidden');

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
    setupInfoToggle();
    fetchPlayers();

    // Refresh data periodically (every 5 minutes)
    setInterval(fetchPlayers, 5 * 60 * 1000);
}

// Start the app when DOM is ready
document.addEventListener('DOMContentLoaded', init);
