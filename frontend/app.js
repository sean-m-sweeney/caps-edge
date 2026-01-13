/**
 * Edge Report - Frontend JavaScript
 * Handles data fetching, table rendering, sorting, and filtering
 */

// State - use single arrays, not duplicates
let forwards = [];
let defensemen = [];
let goalies = [];
let teams = [];
let teamSpeed = null;
let currentView = 'forwards'; // 'forwards', 'defensemen', or 'goalies'
let sortState = { field: 'points', direction: 'desc' };
let goalieSortState = { field: 'wins', direction: 'desc' };

// Virtual scrolling config
const VIRTUAL_SCROLL = {
    rowHeight: 41,      // Approximate row height in pixels
    bufferRows: 5,      // Extra rows to render above/below viewport
    enabled: true       // Toggle virtual scrolling
};
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
const goaliesBody = document.getElementById('goalies-body');
const goalieTableWrapper = document.getElementById('goalie-table-wrapper');
const skaterTableWrapper = document.getElementById('skater-table-wrapper');
const teamSpeedDisplay = document.getElementById('team-speed-display');

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
            <td class="sticky left-0 z-30 bg-void border-r border-grid-line p-3 font-mono font-bold text-white text-center">
                #${player.jersey_number || '-'}
            </td>
            <td class="p-3 font-bold text-white truncate">
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
            <td class="p-3 text-right font-mono ${getPercentileClass(stats.p60_percentile)}">${formatStat(stats.p60, 2)}</td>
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
 * Render a single goalie row
 */
function renderGoalieRow(goalie) {
    const showTeamCol = filterState.type !== 'team';

    return `
        <tr class="table-row group transition-colors">
            <td class="sticky left-0 z-30 bg-void border-r border-grid-line p-3 font-mono font-bold text-white text-center">
                #${goalie.jersey_number || '-'}
            </td>
            <td class="p-3 font-bold text-white truncate">
                <a href="${getHockeyRefUrl(goalie.name)}" target="_blank" class="hover:text-neon-cyan hover:underline decoration-neon-cyan underline-offset-4">
                    ${goalie.name}
                </a>
            </td>
            <td class="p-3 text-gray-400 team-col ${showTeamCol ? '' : 'hidden'}">${goalie.team_abbr || '-'}</td>
            <td class="p-3 text-right font-mono text-gray-400">${formatStat(goalie.games_played)}</td>
            <td class="p-3 text-right font-mono text-gray-300">${formatStat(goalie.wins)}</td>
            <td class="p-3 text-right font-mono text-gray-400">${formatStat(goalie.losses)}</td>
            <td class="p-3 text-right font-mono text-gray-400">${formatStat(goalie.ot_losses)}</td>
            <td class="p-3 text-right font-mono text-gray-400 border-r border-grid-line">${formatStat(goalie.shutouts)}</td>
            <td class="p-3 text-right font-mono ${getPercentileClass(goalie.gaa_percentile)}">
                ${formatStat(goalie.goals_against_avg, 2)}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(goalie.save_pct_percentile)}">
                ${goalie.save_pct ? '.' + Math.round(goalie.save_pct * 10).toString().padStart(3, '0') : '-'}
            </td>
            <td class="p-3 text-right font-mono ${getPercentileClass(goalie.hdsv_percentile)}">
                ${goalie.high_danger_save_pct ? '.' + Math.round(goalie.high_danger_save_pct * 10).toString().padStart(3, '0') : '-'}
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
        'p60': 'stats.p60',
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
    if (currentView === 'goalies') {
        // Goalie sorting
        if (goalieSortState.field === field) {
            goalieSortState.direction = goalieSortState.direction === 'asc' ? 'desc' : 'asc';
        } else {
            goalieSortState.field = field;
            goalieSortState.direction = field === 'name' ? 'asc' : 'desc';
        }
        sortGoaliesArray(goalies, field, goalieSortState.direction);
    } else {
        // Skater sorting
        if (sortState.field === field) {
            sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
        } else {
            sortState.field = field;
            sortState.direction = field === 'name' ? 'asc' : 'desc';
        }
        const currentPlayers = currentView === 'forwards' ? forwards : defensemen;
        sortPlayersArray(currentPlayers, field, sortState.direction);
    }
    renderTable();
    updateSortIndicators();
}

/**
 * Sort goalies array by field
 */
function sortGoaliesArray(goaliesArray, field, direction) {
    const fieldMap = {
        'jersey_number': 'jersey_number',
        'name': 'name',
        'team_abbr': 'team_abbr',
        'games_played': 'games_played',
        'wins': 'wins',
        'losses': 'losses',
        'ot_losses': 'ot_losses',
        'shutouts': 'shutouts',
        'goals_against_avg': 'goals_against_avg',
        'save_pct': 'save_pct',
        'high_danger_save_pct': 'high_danger_save_pct'
    };

    const path = fieldMap[field] || field;

    goaliesArray.sort((a, b) => {
        let aVal = a[path];
        let bVal = b[path];

        // Handle nulls - push to end
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        // For GAA, lower is better so invert for desc
        if (field === 'goals_against_avg') {
            const comparison = aVal - bVal;
            return direction === 'asc' ? -comparison : comparison;
        }

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
 * Update sort indicator arrows in headers
 */
function updateSortIndicators() {
    // Remove all existing indicators from both tables
    document.querySelectorAll('.sort-indicator').forEach(el => el.remove());

    if (currentView === 'goalies') {
        const table = document.getElementById('goalies-table');
        const header = table.querySelector(`th[data-sort="${goalieSortState.field}"]`);
        if (header) {
            const indicator = document.createElement('span');
            indicator.className = 'sort-indicator ml-1 text-neon-pink';
            indicator.textContent = goalieSortState.direction === 'asc' ? '▲' : '▼';
            header.appendChild(indicator);
        }
    } else {
        const table = document.getElementById('players-table');
        const header = table.querySelector(`th[data-sort="${sortState.field}"]`);
        if (header) {
            const indicator = document.createElement('span');
            indicator.className = 'sort-indicator ml-1 text-neon-pink';
            indicator.textContent = sortState.direction === 'asc' ? '▲' : '▼';
            header.appendChild(indicator);
        }
    }
}

/**
 * Get current data array based on view
 */
function getCurrentData() {
    if (currentView === 'goalies') return goalies;
    return currentView === 'forwards' ? forwards : defensemen;
}

/**
 * Get current render function based on view
 */
function getCurrentRenderer() {
    return currentView === 'goalies' ? renderGoalieRow : renderPlayerRow;
}

/**
 * Get current tbody element based on view
 */
function getCurrentTbody() {
    return currentView === 'goalies' ? goaliesBody : playersBody;
}

/**
 * Render visible rows with virtual scrolling
 */
function renderVisibleRows(scrollTop = 0) {
    const data = getCurrentData();
    const renderer = getCurrentRenderer();
    const tbody = getCurrentTbody();

    if (!VIRTUAL_SCROLL.enabled || data.length < 50) {
        // For small datasets, render all rows
        tbody.innerHTML = data.map(renderer).join('');
        return;
    }

    const { rowHeight, bufferRows } = VIRTUAL_SCROLL;
    const containerHeight = 600; // Approximate visible height

    // Calculate visible range
    const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - bufferRows);
    const visibleCount = Math.ceil(containerHeight / rowHeight) + (bufferRows * 2);
    const endIndex = Math.min(data.length, startIndex + visibleCount);

    // Render only visible rows
    const visibleData = data.slice(startIndex, endIndex);
    const rowsHtml = visibleData.map(renderer).join('');

    // Calculate padding to maintain scroll position
    const paddingTop = startIndex * rowHeight;
    const paddingBottom = (data.length - endIndex) * rowHeight;

    tbody.innerHTML = `
        <tr style="height: ${paddingTop}px;"><td colspan="20"></td></tr>
        ${rowsHtml}
        <tr style="height: ${paddingBottom}px;"><td colspan="20"></td></tr>
    `;
}

/**
 * Render the current table
 */
function renderTable() {
    if (currentView === 'goalies') {
        // Show goalie table, hide skater table
        skaterTableWrapper.classList.add('hidden');
        goalieTableWrapper.classList.remove('hidden');
        positionCountEl.textContent = `${goalies.length} goalies`;
    } else {
        // Show skater table, hide goalie table
        skaterTableWrapper.classList.remove('hidden');
        goalieTableWrapper.classList.add('hidden');
        const currentPlayers = currentView === 'forwards' ? forwards : defensemen;
        positionCountEl.textContent = `${currentPlayers.length} ${currentView === 'forwards' ? 'forwards' : 'defensemen'}`;
    }

    // Render with virtual scrolling
    renderVisibleRows(0);
}

/**
 * Setup virtual scroll listeners
 */
function setupVirtualScroll() {
    // Throttled scroll handler
    let scrollTimeout;
    const handleScroll = (e) => {
        if (!VIRTUAL_SCROLL.enabled) return;

        clearTimeout(scrollTimeout);
        scrollTimeout = setTimeout(() => {
            renderVisibleRows(e.target.scrollTop);
        }, 16); // ~60fps
    };

    // Add scroll listeners to both table wrappers
    skaterTableWrapper.addEventListener('scroll', handleScroll);
    goalieTableWrapper.addEventListener('scroll', handleScroll);
}

/**
 * Setup sort click handlers for both skater and goalie tables
 */
function setupSortHandlers() {
    // Skater table sort handlers
    document.querySelectorAll('#players-table th[data-sort]').forEach(header => {
        header.addEventListener('click', () => {
            const field = header.dataset.sort;
            sortTable(field);
        });
    });

    // Goalie table sort handlers
    document.querySelectorAll('#goalies-table th[data-sort]').forEach(header => {
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
        positionToggle.addEventListener('change', async () => {
            currentView = positionToggle.value;

            if (currentView === 'goalies') {
                // Fetch goalies if not loaded or team changed
                if (goalies.length === 0) {
                    await fetchGoalies();
                }
                goalieSortState = { field: 'wins', direction: 'desc' };
                sortGoaliesArray(goalies, 'wins', 'desc');
            } else {
                sortState = { field: 'points', direction: 'desc' };
                const currentPlayers = currentView === 'forwards' ? forwards : defensemen;
                sortPlayersArray(currentPlayers, 'points', 'desc');
            }

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
        // Clear old data first (memory optimization)
        forwards.length = 0;
        defensemen.length = 0;
        goalies.length = 0;

        const url = buildPlayersUrl();
        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to fetch players');

        const data = await response.json();

        // Direct assignment to position arrays - no intermediate storage
        for (const p of data.players) {
            if (p.position === 'D') {
                defensemen.push(p);
            } else {
                forwards.push(p);
            }
        }
        // Let data.players be garbage collected immediately
        data.players = null;

        // Update UI
        lastUpdatedEl.textContent = formatRelativeTime(data.last_updated);
        playerCountEl.textContent = forwards.length + defensemen.length;

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

        // Fetch team speed if viewing a specific team
        if (filterState.type === 'team') {
            fetchTeamSpeed(filterState.team);
        } else {
            teamSpeed = null;
            renderTeamSpeed();
        }

    } catch (error) {
        console.error('Error fetching players:', error);
        loadingEl.innerHTML = '<div class="text-red-500 font-arcade text-xs">ERROR LOADING DATA</div>';
    }
}

/**
 * Build API URL for goalies with current filters
 */
function buildGoaliesUrl() {
    let url = '/api/goalies';
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
 * Fetch goalies data from API
 */
async function fetchGoalies() {
    try {
        const url = buildGoaliesUrl();
        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to fetch goalies');

        const data = await response.json();
        goalies = data.goalies || [];

        // Sort by wins by default
        sortGoaliesArray(goalies, 'wins', 'desc');

    } catch (error) {
        console.error('Error fetching goalies:', error);
        goalies = [];
    }
}

/**
 * Fetch team speed stats
 */
async function fetchTeamSpeed(teamAbbr) {
    if (!teamAbbr) {
        teamSpeed = null;
        renderTeamSpeed();
        return;
    }

    try {
        const response = await fetch(`/api/team-speed/${teamAbbr}`);
        if (!response.ok) {
            teamSpeed = null;
        } else {
            teamSpeed = await response.json();
        }
    } catch (error) {
        console.error('Error fetching team speed:', error);
        teamSpeed = null;
    }
    renderTeamSpeed();
}

/**
 * Render team speed display
 */
function renderTeamSpeed() {
    if (!teamSpeedDisplay) return;

    if (!teamSpeed || filterState.type !== 'team') {
        teamSpeedDisplay.classList.add('hidden');
        return;
    }

    teamSpeedDisplay.classList.remove('hidden');
    teamSpeedDisplay.innerHTML = `
        <div class="flex flex-wrap items-center gap-4 md:gap-6 text-xs">
            <div class="flex items-center gap-2">
                <span class="text-gray-500 uppercase tracking-wider">Team Speed</span>
                <span class="font-mono text-neon-cyan font-bold">${teamSpeed.weighted_avg_speed} mph</span>
            </div>
            <div class="flex items-center gap-2">
                <span class="text-gray-500 uppercase tracking-wider">Bursts/Game</span>
                <span class="font-mono text-neon-pink font-bold">${teamSpeed.avg_bursts_per_game}</span>
            </div>
            <div class="flex items-center gap-2">
                <span class="text-gray-500 uppercase tracking-wider">League Rank</span>
                <span class="font-mono ${teamSpeed.rank <= 10 ? 'text-neon-cyan' : teamSpeed.rank >= 22 ? 'text-red-400' : 'text-gray-300'} font-bold">
                    #${teamSpeed.rank}/${teamSpeed.total_teams}
                </span>
            </div>
            <button id="team-speed-info-toggle" class="ml-auto text-gray-500 hover:text-neon-cyan flex items-center gap-1 transition-colors">
                <span class="text-neon-cyan">?</span>
                <span>How is this calculated?</span>
                <span id="team-speed-arrow" class="transition-transform">▼</span>
            </button>
        </div>
        <div id="team-speed-info" class="hidden mt-3 pt-3 border-t border-grid-line text-xs text-gray-400">
            <div class="grid md:grid-cols-3 gap-4">
                <div>
                    <h4 class="text-neon-cyan font-bold mb-1">Team Speed</h4>
                    <p>TOI-weighted average of each player's top speed. Players with more ice time contribute more to the average, reflecting actual on-ice speed presence.</p>
                </div>
                <div>
                    <h4 class="text-neon-pink font-bold mb-1">Bursts/Game</h4>
                    <p>Total team bursts over 20 mph divided by total team games played. Shows how often the team generates explosive skating plays per game.</p>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-1">League Rank</h4>
                    <p>Team's position among all 32 NHL teams, ranked by TOI-weighted average speed. Based on ${teamSpeed.player_count} skaters with 10+ games played.</p>
                </div>
            </div>
        </div>
    `;

    // Add toggle handler
    const toggle = document.getElementById('team-speed-info-toggle');
    const info = document.getElementById('team-speed-info');
    const arrow = document.getElementById('team-speed-arrow');

    if (toggle && info && arrow) {
        toggle.addEventListener('click', () => {
            const isHidden = info.classList.toggle('hidden');
            arrow.style.transform = isHidden ? 'rotate(0deg)' : 'rotate(180deg)';
        });
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
    setupVirtualScroll();
    updateFilterDropdowns();

    await fetchTeams();
    await fetchPlayers();
}

// Start the app
document.addEventListener('DOMContentLoaded', init);
