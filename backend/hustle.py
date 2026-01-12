"""Hustle score calculation for Caps Edge."""

from typing import Optional


def calculate_hustle_score(
    games_played: int,
    avg_toi: float,  # minutes per game
    hits: int,
    bursts_20_plus: int,
    distance_per_game: float,  # miles
    off_zone_time_pct: float,  # percentage (0-100)
    league_max_bursts_per_60: float,
    league_max_distance: float,
    league_max_hits_per_60: float
) -> Optional[float]:
    """
    Calculate hustle score for a player.

    Formula:
    - Calculate per-60-minute rates for bursts and hits
    - Normalize each component against league max
    - Weight: bursts (30%), distance (25%), hits (25%), O-zone time (20%)

    Returns:
        Hustle score (0-100) or None if insufficient data
    """
    # Need minimum data to calculate
    if not all([games_played, avg_toi, avg_toi > 0]):
        return None

    # Calculate total minutes played
    total_minutes = games_played * avg_toi

    if total_minutes <= 0:
        return None

    # Calculate per-60 rates
    bursts_per_60 = (bursts_20_plus or 0) / total_minutes * 60
    hits_per_60 = (hits or 0) / total_minutes * 60

    # Handle missing or zero league max values
    if league_max_bursts_per_60 <= 0:
        league_max_bursts_per_60 = 1
    if league_max_distance <= 0:
        league_max_distance = 1
    if league_max_hits_per_60 <= 0:
        league_max_hits_per_60 = 1

    # Calculate hustle score components (capped at 1.0 for each)
    bursts_component = min(bursts_per_60 / league_max_bursts_per_60, 1.0) * 0.30
    distance_component = min((distance_per_game or 0) / league_max_distance, 1.0) * 0.25
    hits_component = min(hits_per_60 / league_max_hits_per_60, 1.0) * 0.25
    ozone_component = ((off_zone_time_pct or 0) / 100) * 0.20

    # Sum and scale to 0-100
    hustle_score = (bursts_component + distance_component + hits_component + ozone_component) * 100

    return round(hustle_score, 2)


def calculate_league_maxes(players_data: list) -> dict:
    """
    Calculate league maximum values for hustle score normalization.

    Args:
        players_data: List of player dicts with stats

    Returns:
        Dict with max values for bursts_per_60, distance, hits_per_60
    """
    max_bursts_per_60 = 0.0
    max_distance = 0.0
    max_hits_per_60 = 0.0

    for player in players_data:
        games_played = player.get("games_played", 0)
        avg_toi = player.get("avg_toi", 0)

        if not games_played or not avg_toi or avg_toi <= 0:
            continue

        # Skip players with too few games
        if games_played < 10:
            continue

        total_minutes = games_played * avg_toi

        # Calculate per-60 rates
        bursts = player.get("bursts_20_plus", 0) or 0
        hits = player.get("hits", 0) or 0
        distance = player.get("distance_per_game_miles", 0) or 0

        bursts_per_60 = bursts / total_minutes * 60 if total_minutes > 0 else 0
        hits_per_60 = hits / total_minutes * 60 if total_minutes > 0 else 0

        max_bursts_per_60 = max(max_bursts_per_60, bursts_per_60)
        max_distance = max(max_distance, distance)
        max_hits_per_60 = max(max_hits_per_60, hits_per_60)

    return {
        "max_bursts_per_60": max_bursts_per_60,
        "max_distance": max_distance,
        "max_hits_per_60": max_hits_per_60
    }


def calculate_percentile(value: float, sorted_values: list) -> int:
    """
    Calculate percentile rank for a value in a sorted list.

    Args:
        value: The value to find percentile for
        sorted_values: List of values sorted ascending

    Returns:
        Percentile (0-100)
    """
    if not sorted_values or value is None:
        return 0

    count_below = sum(1 for v in sorted_values if v < value)
    percentile = (count_below / len(sorted_values)) * 100

    return int(round(percentile))
