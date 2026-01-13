/**
 * Chel Edge - Frontend JavaScript
 * Handles data fetching, table rendering, sorting, and filtering
 */

// State
let allPlayers = [];
let forwards = [];
let defensemen = [];
let teams = [];
let currentView = 'forwards'; // 'forwards' or 'defensemen'
let sortState = { field: 'points', direction: 'desc' };
let filterState = {
    type: 'team',
    team: 'WSH',
    division: 'Metropolitan',
    conference: 'Eastern'
};

// DOM Elements
const loadingEl = document.getElementById('loading');
const tableContainer = document.getElementById('table-container');
const playersBody = document.getElementById('players-body');
const lastUpdatedEl = document.getElementById('last-updated');
const infoToggle = document.getElementById('info-toggle');
const infoContent = document.getElementById('info-content');
const infoArrow = document.getElementById('info-arrow');
const filterTypeEl = document.getElementById('filter-type');
const filterTeamEl = document.getElementById('filter-team');
const filterDivisionEl = document.getElementById('filter-division');
const filterConferenceEl = document.getElementById('filter-conference');
const playerCountEl = document.getElementById('player-count');
const positionToggle = document.getElementById('position-toggle');
const positionCountEl = document.getElementById('position-count');

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
    if (diffMins < 60) return `Updated ${diffMins} min ago`;
    if (diffHours < 24) return `Updated ${diffHours} hr ago`;
    return `Updated ${diffDays} day${diffDays === 1 ? '' : 's'} ago`;
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
 * Get CSS class for percentile tier (0-100 -> pct-0 through pct-90)
 */
function getPercentileClass(pct) {
    if (pct === null || pct === undefined) return 'text-gray-600';
    if (pct >= 90) return 'pct-90';
    if (pct >= 80) return 'pct-80';
    if (pct >= 70) return 'pct-70';
    if (pct >= 60) return 'pct-60';
    if (pct >= 50) return 'pct-50';
    if (pct >= 40) return 'pct-40';
    if (pct >= 30) return 'pct-30';
    if (pct >= 20) return 'pct-20';
    if (pct >= 10) return 'pct-10';
    return 'pct-0';
}

/**
 * Render a single player row
 */
function renderPlayerRow(player) {
    const stats = player.stats || {};
    const edge = player.edge_stats || {};
    const showTeamCol = filterState.type !== 'team';

    return `
        <tr class="table-row group transition-colors">
            <td class="sticky left-0 z-30 bg-void border-r border-grid-line p-3 font-mono text-gray-500 text-center">
                ${player.jersey_number || '-'}
            </td>
            <td class="sticky left-[50px] z-30 bg-void border-r-2 border-neon-cyan/20 p-3 font-bold text-white truncate">
                <a href="${getHockeyRefUrl(player.name)}" target="_blank" class="hover:text-neon-cyan hover:underline decoration-neon-cyan underline-offset-4">
                    ${player.name}
                </a>
            </td>
            <td class="p-3 text-gray-400 team-col ${showTeamCol ? '' : 'hidden'}">${player.team_abbr || '-'}</td>
            <td class="p-3 text-gray-400 text-center">${player.position}</td>
            <td class="p-3 text-right font-mono text-gray-400">${formatStat(stats.games_played)}</td>
            <td class="p-3 text-right font-mono text-gray-300">${formatStat(stats.avg_toi, 1)}</td>
            <td class="p-3 text-right font-mono text-gray-400">${formatStat(stats.goals)}</td>
            <td class="p-3 text-right font-mono text-gray-400">${formatStat(stats.assists)}</td>
            <td class="p-3 text-right font-mono text-white font-bold">${formatStat(stats.points)}</td>
            <td class="p-3 text-right font-mono text-gray-400 border-r border-grid-line">${formatStat(stats.hits)}</td>
            <td class="p-3 text-right font-mono ${getPercentileClass(edge.top_speed_percentile)}">
                ${formatStat(edge.top_speed_mph, 1)}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(edge.bursts_20_percentile)}">
                ${formatStat(edge.bursts_20_plus, 0)}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(edge.bursts_22_percentile)}">
                ${formatStat(edge.bursts_22_plus, 0)}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(edge.distance_percentile)} border-r border-grid-line">
                ${formatStat(edge.distance_per_game_miles, 2)}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(edge.shot_speed_percentile)}">
                ${formatStat(edge.top_shot_speed_mph, 1)}
            </td>
            <td class="p-3 text-right font-mono text-gray-400 border-r border-grid-line">
                ${formatStat(stats.shots_per_60, 1)}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(edge.off_zone_percentile)}">
                ${edge.off_zone_time_pct ? formatStat(edge.off_zone_time_pct, 1) + '%' : '-'}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(edge.zone_starts_percentile)}">
                ${edge.zone_starts_off_pct ? formatStat(edge.zone_starts_off_pct, 1) + '%' : '-'}
            </td>
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
    const fieldMap = {
        'jersey_number': 'jersey_number',
        'name': 'name',
        'team_abbr': 'team_abbr',
        'position': 'position',
        'games_played': 'stats.games_played',
        'avg_toi': 'stats.avg_toi',
        'goals': 'stats.goals',
        'assists': 'stats.assists',
        'points': 'stats.points',
        'hits': 'stats.hits',
        'shots_per_60': 'stats.shots_per_60',
        'top_speed_mph': 'edge_stats.top_speed_mph',
        'bursts_20_plus': 'edge_stats.bursts_20_plus',
        'bursts_22_plus': 'edge_stats.bursts_22_plus',
        'distance_per_game_miles': 'edge_stats.distance_per_game_miles',
        'off_zone_time_pct': 'edge_stats.off_zone_time_pct',
        'zone_starts_off_pct': 'edge_stats.zone_starts_off_pct',
        'top_shot_speed_mph': 'edge_stats.top_shot_speed_mph'
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
 * Sort the current table
 */
function sortTable(field) {
    // Toggle direction if same field
    if (sortState.field === field) {
        sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
        sortState.field = field;
        sortState.direction = field === 'name' ? 'asc' : 'desc';
    }

    const currentPlayers = currentView === 'forwards' ? forwards : defensemen;
    sortPlayersArray(currentPlayers, field, sortState.direction);
    renderTable();
    updateSortIndicators();
}

/**
 * Update sort indicator arrows in headers
 */
function updateSortIndicators() {
    const table = document.getElementById('players-table');

    // Remove all existing indicators
    table.querySelectorAll('.sort-indicator').forEach(el => el.remove());

    // Add indicator to current sort column
    const header = table.querySelector(`th[data-sort="${sortState.field}"]`);
    if (header) {
        const indicator = document.createElement('span');
        indicator.className = 'sort-indicator ml-1 text-neon-pink';
        indicator.textContent = sortState.direction === 'asc' ? '▲' : '▼';
        header.appendChild(indicator);
    }
}

/**
 * Render the current table
 */
function renderTable() {
    const currentPlayers = currentView === 'forwards' ? forwards : defensemen;
    playersBody.innerHTML = currentPlayers.map(renderPlayerRow).join('');

    // Update position count
    positionCountEl.textContent = `${currentPlayers.length} players`;
}

/**
 * Setup sort click handlers
 */
function setupSortHandlers() {
    document.querySelectorAll('th[data-sort]').forEach(header => {
        header.addEventListener('click', () => {
            const field = header.dataset.sort;
            sortTable(field);
        });
    });
}

/**
 * Setup info section toggle
 */
function setupInfoToggle() {
    if (infoToggle && infoContent) {
        infoToggle.addEventListener('click', () => {
            const isHidden = infoContent.classList.toggle('hidden');
            infoArrow.style.transform = isHidden ? 'rotate(0deg)' : 'rotate(180deg)';
        });
    }
}

/**
 * Setup position toggle
 */
function setupPositionToggle() {
    if (positionToggle) {
        positionToggle.addEventListener('change', () => {
            currentView = positionToggle.value;
            sortState = { field: 'points', direction: 'desc' };

            const currentPlayers = currentView === 'forwards' ? forwards : defensemen;
            sortPlayersArray(currentPlayers, 'points', 'desc');

            renderTable();
            updateSortIndicators();
        });
    }
}

/**
 * Build API URL with current filters
 */
function buildPlayersUrl() {
    let url = '/api/players';
    const params = new URLSearchParams();

    switch (filterState.type) {
        case 'team':
            params.set('team', filterState.team);
            break;
        case 'division':
            params.set('division', filterState.division);
            break;
        case 'conference':
            params.set('conference', filterState.conference);
            break;
    }

    const queryString = params.toString();
    return queryString ? `${url}?${queryString}` : url;
}

/**
 * Show/hide team column based on filter type
 */
function updateTeamColumnVisibility() {
    const showTeam = filterState.type !== 'team';
    document.querySelectorAll('.team-col').forEach(el => {
        el.classList.toggle('hidden', !showTeam);
    });
}

/**
 * Fetch players data from API
 */
async function fetchPlayers() {
    try {
        const url = buildPlayersUrl();
        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to fetch players');

        const data = await response.json();
        allPlayers = data.players;

        // Split into forwards and defensemen
        forwards = allPlayers.filter(p => ['C', 'L', 'R'].includes(p.position));
        defensemen = allPlayers.filter(p => p.position === 'D');

        // Update UI
        lastUpdatedEl.textContent = formatRelativeTime(data.last_updated);
        playerCountEl.textContent = data.count;

        // Sort by points by default
        sortPlayersArray(forwards, 'points', 'desc');
        sortPlayersArray(defensemen, 'points', 'desc');

        // Update visibility and render
        updateTeamColumnVisibility();
        renderTable();
        updateSortIndicators();

        // Show table, hide loading
        loadingEl.classList.add('hidden');
        tableContainer.classList.remove('hidden');

    } catch (error) {
        console.error('Error fetching players:', error);
        loadingEl.innerHTML = '<div class="text-red-500 font-arcade text-xs">ERROR LOADING DATA</div>';
    }
}

/**
 * Fetch teams list and populate dropdown
 */
async function fetchTeams() {
    try {
        const response = await fetch('/api/teams');
        if (!response.ok) throw new Error('Failed to fetch teams');

        const data = await response.json();
        teams = data.teams;

        // Group by division
        const teamsByDivision = {};
        teams.forEach(team => {
            if (!teamsByDivision[team.division]) teamsByDivision[team.division] = [];
            teamsByDivision[team.division].push(team);
        });

        // Build dropdown
        filterTeamEl.innerHTML = '';
        ['Metropolitan', 'Atlantic', 'Central', 'Pacific'].forEach(division => {
            const group = document.createElement('optgroup');
            group.label = division;
            (teamsByDivision[division] || []).forEach(team => {
                const option = document.createElement('option');
                option.value = team.abbr;
                option.textContent = team.name;
                if (team.abbr === filterState.team) option.selected = true;
                group.appendChild(option);
            });
            filterTeamEl.appendChild(group);
        });

    } catch (error) {
        console.error('Error fetching teams:', error);
    }
}

/**
 * Update filter dropdown visibility
 */
function updateFilterDropdowns() {
    filterTeamEl.classList.add('hidden');
    filterDivisionEl.classList.add('hidden');
    filterConferenceEl.classList.add('hidden');

    switch (filterState.type) {
        case 'team': filterTeamEl.classList.remove('hidden'); break;
        case 'division': filterDivisionEl.classList.remove('hidden'); break;
        case 'conference': filterConferenceEl.classList.remove('hidden'); break;
    }
}

/**
 * Setup filter event handlers
 */
function setupFilterHandlers() {
    filterTypeEl.addEventListener('change', () => {
        filterState.type = filterTypeEl.value;
        updateFilterDropdowns();
        fetchPlayers();
    });

    filterTeamEl.addEventListener('change', () => {
        filterState.team = filterTeamEl.value;
        fetchPlayers();
    });

    filterDivisionEl.addEventListener('change', () => {
        filterState.division = filterDivisionEl.value;
        fetchPlayers();
    });

    filterConferenceEl.addEventListener('change', () => {
        filterState.conference = filterConferenceEl.value;
        fetchPlayers();
    });
}

/**
 * Initialize the app
 */
async function init() {
    setupSortHandlers();
    setupInfoToggle();
    setupPositionToggle();
    setupFilterHandlers();
    updateFilterDropdowns();

    await fetchTeams();
    await fetchPlayers();
}

// Start the app
document.addEventListener('DOMContentLoaded', init);
