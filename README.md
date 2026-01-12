# Caps Edge

NHL Edge Stats for Washington Capitals - Advanced skating and effort metrics with league percentile context.

![Screenshot placeholder](screenshot.png)

## Overview

Caps Edge displays NHL Edge tracking statistics for Washington Capitals players, giving fans access to advanced metrics like skating speed, burst counts, zone time, and shot velocity. Every stat includes a league-wide percentile so you can see how each player compares to the rest of the NHL.

Built for sharing on Caps fan communities like Reddit r/caps, Russian Machine Never Breaks, and Japers Rink.

## Features

- **Traditional Stats**: GP, TOI, G, A, P, +/-, Hits, PIM, FO%
- **Edge Stats with Percentiles**:
  - Top skating speed (mph)
  - Bursts over 20 mph and 22 mph
  - Distance skated per game (miles)
  - Offensive/Defensive zone time percentages
  - Zone start percentage
  - Top shot speed (mph)
  - Custom "Hustle Score"
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
- **1:00 PM ET** - After morning skates, before afternoon games
- **7:00 PM ET** - After early games, before late games
- **11:00 PM ET** - After most games complete

You can also trigger a manual refresh using the API endpoint.

## Hustle Score Methodology

The Hustle Score is a custom composite metric that measures player effort and engagement. It combines:

| Component | Weight | Description |
|-----------|--------|-------------|
| Bursts per 60 | 30% | High-speed skating bursts (20+ mph) normalized to 60 minutes |
| Distance per game | 25% | Total miles skated per game |
| Hits per 60 | 25% | Physical engagement normalized to 60 minutes |
| O-Zone time % | 20% | Time spent in offensive zone |

**Calculation:**
1. Calculate per-60-minute rates for bursts and hits
2. Normalize each component against the league maximum
3. Apply weights and sum to get a 0-100 score
4. Calculate percentile against all NHL skaters with 10+ games

This metric rewards players who skate hard, hit often, and spend time in the offensive zone - the qualities that Caps fans appreciate most.

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
- **Inspiration**: The passionate Caps fan community

## Support

If you find this useful, consider buying me a coffee!

[![Ko-fi](https://img.shields.io/badge/Ko--fi-Support-FF5E5B?logo=ko-fi&logoColor=white)](https://ko-fi.com/capsedge)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Disclaimer

This project is not affiliated with, endorsed by, or connected to the NHL, Washington Capitals, or any NHL team. All data is sourced from publicly available NHL APIs.
