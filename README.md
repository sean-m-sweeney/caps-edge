# Chel Edge

NHL Edge stats in a comprehensive, sortable format.

![Screenshot placeholder](screenshot.png)

## Overview

Chel Edge combines traditional NHL stats with NHL Edge tracking data in one sortable view. Filter by team, division, conference, or view the entire league. Every stat includes a league-wide percentile for comparison.

## Features

- **League-Wide Coverage**: View stats for any NHL team, division, conference, or entire league
- **Traditional Stats**: GP, TOI, G, A, P, P/60, +/-, Hits, PIM, Shots/60, FO%
- **Edge Stats with Percentiles**:
  - Top skating speed (mph)
  - Bursts over 20 mph and 22 mph
  - Distance skated per game (miles)
  - Offensive/Defensive zone time percentages
  - Zone start percentage
  - Top shot speed (mph)
- **Interactive Table**: Click any column to sort
- **Percentile Coloring**: Green for 75th+ percentile, red for below 25th
- **Player Links**: Click any player name to view their Hockey-Reference page
- **Auto-refresh**: Data updates daily at 6 AM ET

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
Returns skaters with full stats and edge stats. Supports filtering:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `team` | Team abbreviation | `?team=WSH` |
| `division` | Division name | `?division=Metropolitan` |
| `conference` | Conference name | `?conference=Eastern` |

If no filters provided, returns all league players.

### `GET /api/players/{player_id}`
Returns a single player with all stats.

### `GET /api/teams`
Returns list of all NHL teams with divisions and conferences.

### `GET /api/divisions`
Returns list of divisions grouped by conference.

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

Data is automatically refreshed once daily via cron:
- **6:00 AM ET**

The refresh fetches Edge stats for all ~850 qualified NHL skaters (10+ games played), so it takes approximately 15-20 minutes to complete.

You can also trigger a manual refresh using the API endpoint.

## Tech Stack

- **Backend**: Python 3.11, FastAPI, SQLite
- **Frontend**: Vanilla HTML/CSS/JS with Tailwind CSS (CDN)
- **Data Source**: NHL API via [nhl-api-py](https://github.com/coreyjs/nhl-api-py)
- **Deployment**: Docker container for UNRAID

## Project Structure

```
chel-edge/
├── backend/
│   ├── __init__.py
│   ├── main.py           # FastAPI app and routes
│   ├── database.py       # SQLite setup and queries
│   ├── fetcher.py        # NHL API data fetching
│   └── models.py         # Pydantic models
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
- [x] ~~Support for other NHL teams (team selector)~~ (v2.0)
- [ ] Historical season selector
- [ ] Game-by-game breakdown for individual players
- [ ] Playoff vs regular season toggle
- [ ] Mobile app (PWA)

## Contributing

Contributions welcome! Please open an issue first to discuss proposed changes.

## Credits

- **Data**: [NHL Edge](https://www.nhl.com/stats/edge) via the NHL API
- **API Library**: [nhl-api-py](https://github.com/coreyjs/nhl-api-py) by Corey Schaf

## Support

If you find this useful, consider buying me a coffee!

[![Ko-fi](https://img.shields.io/badge/Ko--fi-Support-FF5E5B?logo=ko-fi&logoColor=white)](https://ko-fi.com/cheledge)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Disclaimer

This project is not affiliated with, endorsed by, or connected to the NHL or any NHL team. All data is sourced from publicly available NHL APIs.
