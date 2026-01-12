"""Pydantic models for API responses."""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class PlayerStats(BaseModel):
    """Traditional player statistics."""
    games_played: Optional[int] = None
    avg_toi: Optional[float] = None  # minutes per game
    goals: Optional[int] = None
    assists: Optional[int] = None
    points: Optional[int] = None
    plus_minus: Optional[int] = None
    hits: Optional[int] = None
    pim: Optional[int] = None
    faceoff_win_pct: Optional[float] = None


class PlayerEdgeStats(BaseModel):
    """NHL Edge tracking statistics."""
    # Skating Speed
    top_speed_mph: Optional[float] = None
    top_speed_percentile: Optional[int] = None

    # Bursts
    bursts_20_plus: Optional[int] = None
    bursts_20_percentile: Optional[int] = None
    bursts_22_plus: Optional[int] = None
    bursts_22_percentile: Optional[int] = None

    # Distance
    distance_per_game_miles: Optional[float] = None
    distance_percentile: Optional[int] = None

    # Zone Time
    off_zone_time_pct: Optional[float] = None
    off_zone_percentile: Optional[int] = None
    def_zone_time_pct: Optional[float] = None
    def_zone_percentile: Optional[int] = None
    neu_zone_time_pct: Optional[float] = None

    # Zone Starts
    zone_starts_off_pct: Optional[float] = None
    zone_starts_percentile: Optional[int] = None

    # Shot Speed
    top_shot_speed_mph: Optional[float] = None
    shot_speed_percentile: Optional[int] = None

    # Calculated
    hustle_score: Optional[float] = None
    hustle_percentile: Optional[int] = None


class Player(BaseModel):
    """Complete player data including all stats."""
    player_id: int
    name: str
    position: str
    jersey_number: Optional[int] = None
    stats: Optional[PlayerStats] = None
    edge_stats: Optional[PlayerEdgeStats] = None


class PlayerResponse(BaseModel):
    """API response for a single player."""
    player: Player
    last_updated: Optional[datetime] = None


class PlayersResponse(BaseModel):
    """API response for all players."""
    players: list[Player]
    last_updated: Optional[datetime] = None
    count: int


class HealthResponse(BaseModel):
    """API health check response."""
    status: str
    last_updated: Optional[datetime] = None
    player_count: int


class RefreshResponse(BaseModel):
    """API refresh response."""
    status: str
    message: str
    players_updated: int
