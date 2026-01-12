"""Motor Index calculation for Caps Edge.

Motor Index measures player effort relative to position average.
Unlike raw stats that favor certain positions, Motor Index compares
each player only to others who play the same role.
"""

from typing import Optional
from collections import defaultdict


def calculate_position_averages(players: list) -> dict:
    """
    Calculate average stats for each position.

    Args:
        players: List of player dicts with stats

    Returns:
        Dict keyed by position with average values
    """
    # Group players by position
    position_stats = defaultdict(list)

    for player in players:
        games_played = player.get("games_played", 0)
        avg_toi = player.get("avg_toi", 0)

        # Skip players with insufficient data
        if games_played < 10 or avg_toi <= 0:
            continue

        position = player.get("position")
        if not position or position == "G":
            continue

        # Map L/R to LW/RW for grouping (but keep original for calculation)
        # Actually, NHL uses C, L, R, D - we'll keep those
        minutes_played = games_played * avg_toi

        # Calculate per-60 rates
        bursts = player.get("bursts_20_plus", 0) or 0
        hits = player.get("hits", 0) or 0
        shots = player.get("shots", 0) or 0
        distance = player.get("distance_per_game_miles", 0) or 0
        off_zone_pct = player.get("off_zone_time_pct", 0) or 0

        bursts_per_60 = (bursts / minutes_played * 60) if minutes_played > 0 else 0
        hits_per_60 = (hits / minutes_played * 60) if minutes_played > 0 else 0
        shots_per_60 = (shots / minutes_played * 60) if minutes_played > 0 else 0

        position_stats[position].append({
            "bursts_per_60": bursts_per_60,
            "distance_per_game": distance,
            "hits_per_60": hits_per_60,
            "shots_per_60": shots_per_60,
            "off_zone_pct": off_zone_pct
        })

    # Calculate averages for each position
    averages = {}
    for position, stats_list in position_stats.items():
        if not stats_list:
            continue

        n = len(stats_list)
        averages[position] = {
            "avg_bursts_per_60": sum(s["bursts_per_60"] for s in stats_list) / n,
            "avg_distance_per_game": sum(s["distance_per_game"] for s in stats_list) / n,
            "avg_hits_per_60": sum(s["hits_per_60"] for s in stats_list) / n,
            "avg_shots_per_60": sum(s["shots_per_60"] for s in stats_list) / n,
            "avg_off_zone_pct": sum(s["off_zone_pct"] for s in stats_list) / n,
            "sample_size": n
        }

    return averages


def calculate_motor_index(
    position: str,
    games_played: int,
    avg_toi: float,
    bursts_20_plus: int,
    distance_per_game: float,
    hits: int,
    shots: int,
    off_zone_time_pct: float,
    position_avgs: dict
) -> Optional[float]:
    """
    Calculate Motor Index for a player.

    Motor Index measures effort relative to position average:
    - Burst component (25%): Speed bursts per 60 minutes
    - Distance component (20%): Miles skated per game
    - Hits component (20%): Physical engagement per 60 minutes
    - Shots component (20%): Shot attempts per 60 minutes
    - O-Zone component (15%): Offensive zone time percentage

    Args:
        position: Player position (C, L, R, D)
        games_played: Total games played
        avg_toi: Average time on ice per game (minutes)
        bursts_20_plus: Total bursts over 20 mph
        distance_per_game: Miles skated per game
        hits: Total hits
        shots: Total shots
        off_zone_time_pct: Offensive zone time percentage
        position_avgs: Dict of position averages

    Returns:
        Motor Index score (0-100) or None if insufficient data
    """
    # Need minimum games and position average data
    if games_played < 10 or avg_toi <= 0:
        return None

    if position not in position_avgs:
        return None

    avg = position_avgs[position]

    # Ensure we have valid averages
    if not all([
        avg.get("avg_bursts_per_60", 0) > 0,
        avg.get("avg_distance_per_game", 0) > 0,
        avg.get("avg_hits_per_60", 0) > 0,
        avg.get("avg_shots_per_60", 0) > 0,
        avg.get("avg_off_zone_pct", 0) > 0
    ]):
        return None

    # Calculate per-60 rates
    minutes_played = games_played * avg_toi
    bursts_per_60 = (bursts_20_plus / minutes_played * 60) if minutes_played > 0 else 0
    hits_per_60 = (hits / minutes_played * 60) if minutes_played > 0 else 0
    shots_per_60 = (shots / minutes_played * 60) if minutes_played > 0 else 0

    # Calculate components (% above/below position average)
    # Each component is (player_rate / avg_rate - 1), which gives a % difference
    # If player is at average, component = 0
    # If player is 50% above average, component = 0.5
    burst_component = (bursts_per_60 / avg["avg_bursts_per_60"] - 1) * 0.25
    distance_component = (distance_per_game / avg["avg_distance_per_game"] - 1) * 0.20
    hits_component = (hits_per_60 / avg["avg_hits_per_60"] - 1) * 0.20
    shots_component = (shots_per_60 / avg["avg_shots_per_60"] - 1) * 0.20
    oz_component = (off_zone_time_pct / avg["avg_off_zone_pct"] - 1) * 0.15

    # Combine and scale to ~0-100 range centered on 50
    raw_score = burst_component + distance_component + hits_component + shots_component + oz_component
    motor_index = (raw_score * 50) + 50

    # Clamp to reasonable range
    motor_index = max(0, min(100, motor_index))

    return round(motor_index, 1)


def calculate_percentile(value: float, sorted_values: list) -> int:
    """
    Calculate percentile rank of a value within a sorted list.

    Args:
        value: The value to rank
        sorted_values: List of values sorted in ascending order

    Returns:
        Percentile rank (0-100)
    """
    if not sorted_values:
        return 50

    # Count how many values are below this one
    below = sum(1 for v in sorted_values if v < value)

    # Calculate percentile
    percentile = (below / len(sorted_values)) * 100

    return int(round(percentile))


def calculate_shots_percentile(shots_per_60: float, all_shots_per_60: list) -> int:
    """
    Calculate percentile for shots per 60.

    Args:
        shots_per_60: Player's shots per 60 minutes
        all_shots_per_60: List of all players' shots per 60

    Returns:
        Percentile rank (0-100)
    """
    if not all_shots_per_60 or shots_per_60 is None:
        return None

    sorted_values = sorted(all_shots_per_60)
    return calculate_percentile(shots_per_60, sorted_values)
