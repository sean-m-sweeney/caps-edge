# Caps Edge

NHL Edge Stats for Washington Capitals - Advanced skating and effort metrics with league percentile context.

![Screenshot placeholder](screenshot.png)

## Overview

Caps Edge displays NHL Edge tracking statistics for Washington Capitals players. Includes skating speed, burst counts, zone time, shot velocity, and a custom Hustle Score. Every stat includes a league-wide percentile for comparison.

## Features

- **Traditional Stats**: GP, TOI, G, A, P, +/-, Hits, PIM, Shots/60, FO%
- **Edge Stats with Percentiles**:
  - Top skating speed (mph)
  - Bursts over 20 mph and 22 mph
  - Distance skated per game (miles)
  - Offensive/Defensive zone time percentages
  - Zone start percentage
  - Top shot speed (mph)
  - Motor Index (effort relative to position average)
- **Interactive Table**: Click any column to sort
- **Percentile Coloring**: Green for 75th+ percentile, red for below 25th
- **Player Links**: Click any player name to view their Hockey-Reference page
- **Auto-refresh**: Data updates 3x daily (1 PM, 7 PM, 11 PM ET)

## Quick Start

### Using Docker Compose (Recommended)

1. Clone the repository:
   ```bash
   git clone https://github.com/sean-m-sweeney/caps-edge.git
   cd caps-edge
   ```

2. Create a `.env` file with your API key:
   ```bash
   echo "API_REFRESH_KEY=your-secret-key-here" > .env
   ```

3. Start the container:
   ```bash
   docker-compose up -d
   ```

4. Access the app at `http://localhost:8000`

The first startup will automatically fetch initial data, which may take a few minutes.

### Manual Installation

1. Install Python 3.11+

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the initial data fetch:
   ```bash
   python scripts/refresh.py
   ```

4. Start the server:
   ```bash
   uvicorn backend.main:app --host 0.0.0.0 --port 8000
   ```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `API_REFRESH_KEY` | Secret key for manual refresh endpoint | `dev-key-change-me` |
| `TZ` | Timezone for cron scheduling | `America/New_York` |
| `DATA_DIR` | Directory for SQLite database | `./data` |

## API Endpoints

### `GET /api/players`
Returns all Caps skaters with full stats and edge stats.

### `GET /api/players/{player_id}`
Returns a single player with all stats.

### `GET /api/health`
Returns service status and last update timestamp.

### `GET /api/refresh`
Triggers a background data refresh. Requires `X-API-Key` header.

```bash
curl -H "X-API-Key: your-secret-key" http://localhost:8000/api/refresh
```

### `GET /api/refresh/sync`
Triggers a synchronous data refresh (waits for completion). Requires `X-API-Key` header.

## Data Refresh Schedule

Data is automatically refreshed 3 times daily via cron:
- **1:00 PM ET**
- **7:00 PM ET**
- **11:00 PM ET**

You can also trigger a manual refresh using the API endpoint.

## Motor Index Methodology

Motor Index measures player effort relative to position average. Unlike raw stats that favor certain positions, Motor Index compares each player only to others who play the same role.

### Components

| Stat | Weight | Rationale |
|------|--------|-----------|
| Speed Bursts/60 | 25% | Explosive effort, backchecking, attacking loose pucks |
| Distance/Game | 20% | Total work output, can't fake skating miles |
| Hits/60 | 20% | Physical engagement, finishing checks |
| Shots/60 | 20% | Offensive aggression, creating chances |
| O-Zone Time % | 15% | Sustained pressure, not floating in neutral zone |

### Calculation

Each component is calculated as a ratio to the player's position average:
- A player exactly at average for all stats scores 50
- Scoring above average increases the index
- Scoring below average decreases it

The final score typically ranges from 25-75, with elite effort players reaching 60+.

### Interpreting Scores

| Score | Meaning |
|-------|---------|
| 60+ | Elite motor. Consistently outworks peers. |
| 50-59 | Above average effort for position. |
| 40-49 | Average. Doing what's expected. |
| Below 40 | Below average activity. May indicate skill-over-effort player or limited role. |

### Limitations

- Measures activity, not efficiency or skill
- Does not account for quality of competition
- Players with limited ice time may have inflated per-60 rates
- Requires 10+ games played for inclusion

## Tech Stack

- **Backend**: Python 3.11, FastAPI, SQLite
- **Frontend**: Vanilla HTML/CSS/JS with Tailwind CSS (CDN)
- **Data Source**: NHL API via [nhl-api-py](https://github.com/coreyjs/nhl-api-py)
- **Deployment**: Docker container for UNRAID

## Project Structure

```
caps-edge/
├── backend/
│   ├── __init__.py
│   ├── main.py           # FastAPI app and routes
│   ├── database.py       # SQLite setup and queries
│   ├── fetcher.py        # NHL API data fetching
│   ├── models.py         # Pydantic models
│   └── hustle.py         # Hustle score calculation
├── frontend/
│   ├── index.html        # Main page
│   ├── app.js            # Data fetching and table rendering
│   └── styles.css        # Custom styles
├── scripts/
│   └── refresh.py        # Cron refresh script
├── data/                 # SQLite database (Docker volume)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── crontab
└── README.md
```

## Future Enhancements

- [ ] League-wide leaderboard view for any stat
- [ ] Player comparison tool (side-by-side)
- [ ] Support for other NHL teams (team selector)
- [ ] Historical season selector
- [ ] Game-by-game breakdown for individual players
- [ ] Playoff vs regular season toggle
- [ ] Mobile app (PWA)

## Contributing

Contributions welcome! Please open an issue first to discuss proposed changes.

## Credits

- **Data**: [NHL Edge](https://www.nhl.com/stats/edge) via the NHL API
- **API Library**: [nhl-api-py](https://github.com/coreyjs/nhl-api-py) by Corey Schaf
- **Inspiration**: Caps fan community

## Support

If you find this useful, consider buying me a coffee!

[![Ko-fi](https://img.shields.io/badge/Ko--fi-Support-FF5E5B?logo=ko-fi&logoColor=white)](https://ko-fi.com/capsedge)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Disclaimer

This project is not affiliated with, endorsed by, or connected to the NHL, Washington Capitals, or any NHL team. All data is sourced from publicly available NHL APIs.
