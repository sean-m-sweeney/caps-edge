"""FastAPI application for Caps Edge."""

import os
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from backend import database
from backend.models import (
    Player, PlayerStats, PlayerEdgeStats,
    PlayerResponse, PlayersResponse, HealthResponse, RefreshResponse
)
from backend.fetcher import refresh_data

# API key for protected endpoints
API_REFRESH_KEY = os.environ.get("API_REFRESH_KEY", "dev-key-change-me")

app = FastAPI(
    title="Caps Edge API",
    description="NHL Edge Stats for Washington Capitals",
    version="1.0.0"
)

# Static files path
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


def db_row_to_player(row: dict) -> Player:
    """Convert database row to Player model."""
    stats = None
    if row.get("games_played") is not None:
        stats = PlayerStats(
            games_played=row.get("games_played"),
            avg_toi=round(row["avg_toi"], 1) if row.get("avg_toi") else None,
            goals=row.get("goals"),
            assists=row.get("assists"),
            points=row.get("points"),
            plus_minus=row.get("plus_minus"),
            hits=row.get("hits"),
            pim=row.get("pim"),
            faceoff_win_pct=round(row["faceoff_win_pct"] * 100, 1) if row.get("faceoff_win_pct") else None,
            shots=row.get("shots"),
            shots_per_60=round(row["shots_per_60"], 1) if row.get("shots_per_60") else None
        )

    edge_stats = None
    if row.get("top_speed_mph") is not None or row.get("bursts_20_plus") is not None:
        edge_stats = PlayerEdgeStats(
            top_speed_mph=round(row["top_speed_mph"], 1) if row.get("top_speed_mph") else None,
            top_speed_percentile=row.get("top_speed_percentile"),
            bursts_20_plus=row.get("bursts_20_plus"),
            bursts_20_percentile=row.get("bursts_20_percentile"),
            bursts_22_plus=row.get("bursts_22_plus"),
            bursts_22_percentile=row.get("bursts_22_percentile"),
            distance_per_game_miles=round(row["distance_per_game_miles"], 2) if row.get("distance_per_game_miles") else None,
            distance_percentile=row.get("distance_percentile"),
            off_zone_time_pct=round(row["off_zone_time_pct"], 1) if row.get("off_zone_time_pct") else None,
            off_zone_percentile=row.get("off_zone_percentile"),
            def_zone_time_pct=round(row["def_zone_time_pct"], 1) if row.get("def_zone_time_pct") else None,
            def_zone_percentile=row.get("def_zone_percentile"),
            neu_zone_time_pct=round(row["neu_zone_time_pct"], 1) if row.get("neu_zone_time_pct") else None,
            zone_starts_off_pct=round(row["zone_starts_off_pct"], 1) if row.get("zone_starts_off_pct") else None,
            zone_starts_percentile=row.get("zone_starts_percentile"),
            top_shot_speed_mph=round(row["top_shot_speed_mph"], 1) if row.get("top_shot_speed_mph") else None,
            shot_speed_percentile=row.get("shot_speed_percentile"),
            shots_percentile=row.get("shots_percentile"),
            motor_index=round(row["motor_index"], 1) if row.get("motor_index") else None,
            motor_percentile=row.get("motor_percentile")
        )

    return Player(
        player_id=row["player_id"],
        name=row["name"],
        position=row["position"],
        jersey_number=row.get("jersey_number"),
        stats=stats,
        edge_stats=edge_stats
    )


@app.get("/api/players", response_model=PlayersResponse)
async def get_players():
    """Get all Caps skaters with full stats and edge stats."""
    rows = database.get_all_players_with_stats()
    players = [db_row_to_player(row) for row in rows]
    last_updated = database.get_last_updated()

    return PlayersResponse(
        players=players,
        last_updated=last_updated,
        count=len(players)
    )


@app.get("/api/players/{player_id}", response_model=PlayerResponse)
async def get_player(player_id: int):
    """Get a single player with all stats."""
    row = database.get_player_by_id(player_id)
    if not row:
        raise HTTPException(status_code=404, detail="Player not found")

    player = db_row_to_player(row)
    last_updated = database.get_last_updated()

    return PlayerResponse(
        player=player,
        last_updated=last_updated
    )


@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    rows = database.get_all_players_with_stats()
    last_updated = database.get_last_updated()

    return HealthResponse(
        status="healthy",
        last_updated=last_updated,
        player_count=len(rows)
    )


@app.get("/api/refresh", response_model=RefreshResponse)
async def trigger_refresh(
    background_tasks: BackgroundTasks,
    x_api_key: str = Header(None)
):
    """Trigger a manual data refresh. Protected by API key."""
    if x_api_key != API_REFRESH_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Run refresh in background
    background_tasks.add_task(refresh_data)

    return RefreshResponse(
        status="started",
        message="Data refresh started in background",
        players_updated=0
    )


@app.get("/api/refresh/sync", response_model=RefreshResponse)
async def trigger_refresh_sync(x_api_key: str = Header(None)):
    """Trigger a synchronous data refresh. Protected by API key."""
    if x_api_key != API_REFRESH_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    players_updated = refresh_data()

    return RefreshResponse(
        status="completed",
        message="Data refresh completed",
        players_updated=players_updated
    )


# Serve frontend static files
@app.get("/")
async def serve_index():
    """Serve the main index.html."""
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    raise HTTPException(status_code=404, detail="Frontend not found")


@app.get("/{filename:path}")
async def serve_static(filename: str):
    """Serve static files from frontend directory."""
    # Skip API routes
    if filename.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")

    file_path = FRONTEND_DIR / filename
    if file_path.exists() and file_path.is_file():
        return FileResponse(file_path)

    # Fall back to index.html for SPA routing
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)

    raise HTTPException(status_code=404, detail="File not found")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
