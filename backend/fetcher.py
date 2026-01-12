"""NHL API data fetching for Caps Edge."""

import logging
from datetime import datetime
from typing import Optional
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder
from nhlpy.api.query.filters.game_type import GameTypeQuery
from nhlpy.api.query.filters.season import SeasonQuery

from backend import database
from backend.motor import (
    calculate_motor_index,
    calculate_position_averages,
    calculate_percentile,
    calculate_shots_percentile
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Washington Capitals
CAPS_TEAM_ABBR = "WSH"
CAPS_FRANCHISE_ID = "24"

# Current season
CURRENT_SEASON = "20252026"


def get_current_season() -> str:
    """Get the current NHL season string."""
    now = datetime.now()
    # NHL season starts in October
    if now.month >= 10:
        return f"{now.year}{now.year + 1}"
    else:
        return f"{now.year - 1}{now.year}"


def fetch_caps_roster(client: NHLClient) -> list:
    """
    Fetch current Washington Capitals roster.

    Returns:
        List of player dicts with id, name, position, jersey_number
    """
    logger.info("Fetching Caps roster...")
    season = get_current_season()

    try:
        roster = client.teams.team_roster(team_abbr=CAPS_TEAM_ABBR, season=season)
    except Exception as e:
        logger.error(f"Error fetching roster: {e}")
        return []

    players = []

    # Process forwards
    for player in roster.get("forwards", []):
        players.append({
            "player_id": player["id"],
            "name": f"{player['firstName']['default']} {player['lastName']['default']}",
            "position": player["positionCode"],
            "jersey_number": player.get("sweaterNumber")
        })

    # Process defensemen
    for player in roster.get("defensemen", []):
        players.append({
            "player_id": player["id"],
            "name": f"{player['firstName']['default']} {player['lastName']['default']}",
            "position": player["positionCode"],
            "jersey_number": player.get("sweaterNumber")
        })

    # Skip goalies for v1
    logger.info(f"Found {len(players)} skaters on roster")
    return players


def fetch_traditional_stats(client: NHLClient, player_ids: list) -> dict:
    """
    Fetch traditional stats for players.

    Returns:
        Dict mapping player_id to stats dict
    """
    logger.info("Fetching traditional stats...")
    season = get_current_season()

    try:
        # Paginate through all summary stats (API caps at 100 per request)
        all_summary = []
        for start in range(0, 1000, 100):
            batch = client.stats.skater_stats_summary(
                start_season=season,
                end_season=season,
                game_type_id=2,
                limit=100,
                start=start
            )
            if not batch:
                break
            all_summary.extend(batch)
            logger.info(f"Fetched summary stats batch: {len(batch)} players (offset {start})")

        summary_data = {p["playerId"]: p for p in all_summary}
        logger.info(f"Total summary stats: {len(summary_data)} players")

        # Paginate through realtime stats (hits, blocks, etc.)
        # Use the query context method for realtime since skater_stats_summary doesn't have it
        filters = [
            SeasonQuery(season_start=season, season_end=season),
            GameTypeQuery(game_type="2"),
        ]
        qb = QueryBuilder()
        query_ctx = qb.build(filters=filters)

        all_realtime = []
        for start in range(0, 1000, 100):
            result = client.stats.skater_stats_with_query_context(
                query_context=query_ctx,
                report_type="realtime",
                limit=100,
                start=start
            )
            batch = result.get("data", [])
            if not batch:
                break
            all_realtime.extend(batch)

        realtime_data = {p["playerId"]: p for p in all_realtime}
        logger.info(f"Total realtime stats: {len(realtime_data)} players")

    except Exception as e:
        logger.error(f"Error fetching traditional stats: {e}")
        return {}

    stats = {}
    for player_id in player_ids:
        summary = summary_data.get(player_id, {})
        realtime = realtime_data.get(player_id, {})

        if not summary:
            logger.warning(f"No summary stats for player {player_id} - may not have played this season")
            # Still create entry with null values so player shows in table
            stats[player_id] = {
                "games_played": 0,
                "avg_toi": None,
                "goals": 0,
                "assists": 0,
                "points": 0,
                "plus_minus": 0,
                "hits": 0,
                "pim": 0,
                "faceoff_win_pct": None,
                "shots": 0,
                "shots_per_60": None
            }
            continue

        # TOI is in seconds, convert to minutes
        toi_seconds = summary.get("timeOnIcePerGame", 0)
        avg_toi = toi_seconds / 60 if toi_seconds else None

        # Calculate shots per 60
        games_played = summary.get("gamesPlayed", 0)
        shots = summary.get("shots", 0)
        shots_per_60 = None
        if games_played > 0 and avg_toi and avg_toi > 0:
            minutes_played = games_played * avg_toi
            shots_per_60 = round((shots / minutes_played) * 60, 2) if minutes_played > 0 else None

        stats[player_id] = {
            "games_played": games_played,
            "avg_toi": avg_toi,
            "goals": summary.get("goals"),
            "assists": summary.get("assists"),
            "points": summary.get("points"),
            "plus_minus": summary.get("plusMinus"),
            "hits": realtime.get("hits"),
            "pim": summary.get("penaltyMinutes"),
            "faceoff_win_pct": summary.get("faceoffWinPct"),
            "shots": shots,
            "shots_per_60": shots_per_60
        }

    logger.info(f"Got traditional stats for {len(stats)} players")
    return stats


def fetch_edge_stats(client: NHLClient, player_id: int) -> Optional[dict]:
    """
    Fetch Edge stats for a single player.

    Returns:
        Dict with Edge stats or None if not available
    """
    try:
        # Get main Edge detail
        detail = client.edge.skater_detail(player_id=str(player_id))

        # Get speed detail for bursts over 22
        speed_detail = client.edge.skater_skating_speed_detail(player_id=str(player_id))

        # Get zone time for zone starts
        zone_detail = client.edge.skater_zone_time(player_id=str(player_id))

    except Exception as e:
        logger.warning(f"Error fetching Edge stats for player {player_id}: {e}")
        return None

    if not detail:
        return None

    # Extract skating speed data
    skating = detail.get("skatingSpeed", {})
    speed_max = skating.get("speedMax", {})
    bursts_20 = skating.get("burstsOver20", {})

    # Extract speed detail for bursts over 22
    speed_details = speed_detail.get("skatingSpeedDetails", {}) if speed_detail else {}
    bursts_22 = speed_details.get("burstsOver22", {})

    # Extract distance
    distance = detail.get("totalDistanceSkated", {})

    # Calculate distance per game
    player_info = detail.get("player", {})
    games_played = player_info.get("gamesPlayed", 1)
    total_distance = distance.get("imperial", 0)
    distance_per_game = total_distance / games_played if games_played > 0 else 0

    # Extract zone time
    zone_time = detail.get("zoneTimeDetails", {})

    # Extract zone starts
    zone_starts = zone_detail.get("zoneStarts", {}) if zone_detail else {}

    # Extract shot speed
    shot_speed = detail.get("topShotSpeed", {})

    # Convert percentiles from decimal (0-1) to int (0-100)
    def to_pct(val):
        if val is None:
            return None
        return int(round(val * 100))

    return {
        "top_speed_mph": speed_max.get("imperial"),
        "top_speed_percentile": to_pct(speed_max.get("percentile")),
        "bursts_20_plus": bursts_20.get("value"),
        "bursts_20_percentile": to_pct(bursts_20.get("percentile")),
        "bursts_22_plus": bursts_22.get("value"),
        "bursts_22_percentile": to_pct(bursts_22.get("percentile")),
        "distance_per_game_miles": round(distance_per_game, 2) if distance_per_game else None,
        "distance_percentile": to_pct(distance.get("percentile")),
        "off_zone_time_pct": round(zone_time.get("offensiveZonePctg", 0) * 100, 1) if zone_time.get("offensiveZonePctg") else None,
        "off_zone_percentile": to_pct(zone_time.get("offensiveZonePercentile")),
        "def_zone_time_pct": round(zone_time.get("defensiveZonePctg", 0) * 100, 1) if zone_time.get("defensiveZonePctg") else None,
        "def_zone_percentile": to_pct(zone_time.get("defensiveZonePercentile")),
        "neu_zone_time_pct": round(zone_time.get("neutralZonePctg", 0) * 100, 1) if zone_time.get("neutralZonePctg") else None,
        "zone_starts_off_pct": round(zone_starts.get("offensiveZoneStartsPctg", 0) * 100, 1) if zone_starts.get("offensiveZoneStartsPctg") else None,
        "zone_starts_percentile": to_pct(zone_starts.get("offensiveZoneStartsPctgPercentile")),
        "top_shot_speed_mph": shot_speed.get("imperial"),
        "shot_speed_percentile": to_pct(shot_speed.get("percentile"))
    }


def fetch_league_stats_for_motor(client: NHLClient) -> list:
    """
    Fetch league-wide stats needed for Motor Index calculation.

    Returns:
        List of player dicts with stats needed for Motor calculation
    """
    logger.info("Fetching league-wide stats for Motor Index calculation...")
    season = get_current_season()

    filters = [
        SeasonQuery(season_start=season, season_end=season),
        GameTypeQuery(game_type="2"),
    ]

    qb = QueryBuilder()
    query_ctx = qb.build(filters=filters)

    try:
        # Get all skaters with summary stats (paginate)
        all_summary = []
        for start in range(0, 1000, 100):
            result = client.stats.skater_stats_with_query_context(
                query_context=query_ctx,
                report_type="summary",
                limit=100,
                start=start
            )
            batch = result.get("data", [])
            if not batch:
                break
            all_summary.extend(batch)

        summary_data = {p["playerId"]: p for p in all_summary}
        logger.info(f"Fetched {len(summary_data)} players for Motor calculation")

        # Get realtime stats (hits)
        all_realtime = []
        for start in range(0, 1000, 100):
            result = client.stats.skater_stats_with_query_context(
                query_context=query_ctx,
                report_type="realtime",
                limit=100,
                start=start
            )
            batch = result.get("data", [])
            if not batch:
                break
            all_realtime.extend(batch)

        realtime_data = {p["playerId"]: p for p in all_realtime}

    except Exception as e:
        logger.error(f"Error fetching league stats: {e}")
        return []

    players = []
    for player_id, summary in summary_data.items():
        games_played = summary.get("gamesPlayed", 0)
        if games_played < 10:
            continue

        toi_seconds = summary.get("timeOnIcePerGame", 0)
        avg_toi = toi_seconds / 60 if toi_seconds else 0

        realtime = realtime_data.get(player_id, {})

        players.append({
            "player_id": player_id,
            "position": summary.get("positionCode"),
            "games_played": games_played,
            "avg_toi": avg_toi,
            "hits": realtime.get("hits", 0),
            "shots": summary.get("shots", 0)
        })

    logger.info(f"Got league stats for {len(players)} qualified players")
    return players


def refresh_data():
    """
    Main refresh function - fetches all data and updates database.

    Returns:
        Number of players updated
    """
    logger.info("Starting data refresh...")
    client = NHLClient()

    # 1. Get Caps roster
    roster = fetch_caps_roster(client)
    if not roster:
        logger.error("Failed to fetch roster")
        return 0

    # Save players to database
    for player in roster:
        database.upsert_player(
            player_id=player["player_id"],
            name=player["name"],
            position=player["position"],
            jersey_number=player["jersey_number"]
        )

    player_ids = [p["player_id"] for p in roster]

    # 2. Get traditional stats
    trad_stats = fetch_traditional_stats(client, player_ids)

    # 3. Get Edge stats for each player
    edge_stats = {}
    for player_id in player_ids:
        stats = fetch_edge_stats(client, player_id)
        if stats:
            edge_stats[player_id] = stats
        logger.info(f"Fetched Edge stats for player {player_id}")

    # 4. Get league-wide stats for Motor Index calculation
    league_players = fetch_league_stats_for_motor(client)

    # 5. Fetch Edge stats for league players (sample for position averages)
    logger.info("Fetching Edge stats for league Motor Index calculation...")
    league_with_edge = []
    sample_count = 0
    max_samples = 150  # Sample 150 players for position averages

    for player in league_players:
        player_id = player["player_id"]
        if sample_count < max_samples:
            edge = fetch_edge_stats(client, player_id)
            if edge:
                player["bursts_20_plus"] = edge.get("bursts_20_plus", 0)
                player["distance_per_game_miles"] = edge.get("distance_per_game_miles", 0)
                player["off_zone_time_pct"] = edge.get("off_zone_time_pct", 0)
                league_with_edge.append(player)
                sample_count += 1
        else:
            break

    logger.info(f"Collected Edge stats for {len(league_with_edge)} league players")

    # 6. Calculate position averages
    position_avgs = calculate_position_averages(league_with_edge)
    logger.info(f"Position averages: {position_avgs}")

    # Save position averages to database
    database.clear_position_averages()
    for position, avgs in position_avgs.items():
        database.insert_position_averages(position, avgs)

    # 7. Calculate Motor Index for all league players and store
    database.clear_league_stats()
    all_motor_scores = []
    all_shots_per_60 = []

    for player in league_with_edge:
        # Calculate shots_per_60 for this player
        games_played = player.get("games_played", 0)
        avg_toi = player.get("avg_toi", 0)
        shots = player.get("shots", 0)

        if games_played > 0 and avg_toi > 0:
            minutes_played = games_played * avg_toi
            shots_per_60 = (shots / minutes_played) * 60 if minutes_played > 0 else 0
            all_shots_per_60.append(shots_per_60)
        else:
            shots_per_60 = 0

        motor = calculate_motor_index(
            position=player.get("position"),
            games_played=games_played,
            avg_toi=avg_toi,
            bursts_20_plus=player.get("bursts_20_plus", 0) or 0,
            distance_per_game=player.get("distance_per_game_miles", 0) or 0,
            hits=player.get("hits", 0) or 0,
            shots=shots,
            off_zone_time_pct=player.get("off_zone_time_pct", 0) or 0,
            position_avgs=position_avgs
        )

        if motor is not None:
            player["motor_index"] = motor
            all_motor_scores.append(motor)
            database.insert_league_stats(player["player_id"], player)

    all_motor_scores.sort()
    logger.info(f"Calculated Motor Index for {len(all_motor_scores)} league players")

    # 8. Save Caps player data with Motor Index and percentiles
    players_updated = 0
    for player_id in player_ids:
        # Get player info
        player_info = next((p for p in roster if p["player_id"] == player_id), {})

        # Save traditional stats
        if player_id in trad_stats:
            database.upsert_player_stats(player_id, trad_stats[player_id])

        # Calculate and save Edge stats with Motor Index
        if player_id in edge_stats:
            edge = edge_stats[player_id]
            trad = trad_stats.get(player_id, {})

            # Calculate Motor Index for this player
            motor = calculate_motor_index(
                position=player_info.get("position"),
                games_played=trad.get("games_played", 0) or 0,
                avg_toi=trad.get("avg_toi", 0) or 0,
                bursts_20_plus=edge.get("bursts_20_plus", 0) or 0,
                distance_per_game=edge.get("distance_per_game_miles", 0) or 0,
                hits=trad.get("hits", 0) or 0,
                shots=trad.get("shots", 0) or 0,
                off_zone_time_pct=edge.get("off_zone_time_pct", 0) or 0,
                position_avgs=position_avgs
            )

            if motor is not None:
                edge["motor_index"] = motor
                edge["motor_percentile"] = calculate_percentile(motor, all_motor_scores)

            # Calculate shots percentile
            shots_per_60 = trad.get("shots_per_60")
            if shots_per_60 is not None and all_shots_per_60:
                edge["shots_percentile"] = calculate_shots_percentile(shots_per_60, all_shots_per_60)

            database.upsert_player_edge_stats(player_id, edge)
            players_updated += 1

    # Update timestamp
    database.set_last_updated(datetime.now())
    logger.info(f"Data refresh complete. Updated {players_updated} players.")

    return players_updated


if __name__ == "__main__":
    # Allow running directly for testing
    refresh_data()
