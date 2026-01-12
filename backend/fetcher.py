"""NHL API data fetching for Caps Edge - League Edition."""

import logging
from datetime import datetime
from typing import Optional
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder
from nhlpy.api.query.filters.game_type import GameTypeQuery
from nhlpy.api.query.filters.season import SeasonQuery

from backend import database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Minimum games played to be included
MIN_GAMES_PLAYED = 10

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


def calculate_percentile(value: float, sorted_values: list) -> Optional[int]:
    """
    Calculate percentile ranking for a value within a sorted list.

    Returns percentile as integer 0-100.
    """
    if not sorted_values or value is None:
        return None

    # Count how many values are below this value
    count_below = sum(1 for v in sorted_values if v < value)
    percentile = (count_below / len(sorted_values)) * 100
    return int(round(percentile))


def fetch_all_league_skaters(client: NHLClient) -> list:
    """
    Fetch all NHL skaters with their team info.

    Returns:
        List of player dicts with id, name, position, team_abbr, jersey_number
    """
    logger.info("Fetching all league skaters...")
    season = get_current_season()

    filters = [
        SeasonQuery(season_start=season, season_end=season),
        GameTypeQuery(game_type="2"),
    ]

    qb = QueryBuilder()
    query_ctx = qb.build(filters=filters)

    try:
        # Paginate through all skaters
        all_skaters = []
        for start in range(0, 1500, 100):
            result = client.stats.skater_stats_with_query_context(
                query_context=query_ctx,
                report_type="summary",
                limit=100,
                start=start
            )
            batch = result.get("data", [])
            if not batch:
                break
            all_skaters.extend(batch)
            logger.info(f"Fetched skater batch: {len(batch)} players (offset {start})")

    except Exception as e:
        logger.error(f"Error fetching league skaters: {e}")
        return []

    players = []
    for skater in all_skaters:
        games_played = skater.get("gamesPlayed", 0)
        if games_played < MIN_GAMES_PLAYED:
            continue

        # Get position (skip goalies)
        position = skater.get("positionCode", "")
        if position == "G":
            continue

        team_abbr = skater.get("teamAbbrevs", "")
        # If player played for multiple teams, get the most recent one
        if "," in team_abbr:
            team_abbr = team_abbr.split(",")[-1].strip()

        players.append({
            "player_id": skater.get("playerId"),
            "name": f"{skater.get('skaterFullName', '')}",
            "position": position,
            "team_abbr": team_abbr,
            "jersey_number": None  # Not in summary stats, will fetch from roster if needed
        })

    logger.info(f"Found {len(players)} qualified skaters across the league")
    return players


def fetch_team_rosters(client: NHLClient, team_abbrs: list) -> dict:
    """
    Fetch jersey numbers from team rosters.

    Returns:
        Dict mapping player_id to jersey_number
    """
    logger.info(f"Fetching roster details for {len(team_abbrs)} teams...")
    season = get_current_season()

    jersey_map = {}
    for team_abbr in team_abbrs:
        try:
            roster = client.teams.team_roster(team_abbr=team_abbr, season=season)
            for group in ["forwards", "defensemen"]:
                for player in roster.get(group, []):
                    jersey_map[player["id"]] = player.get("sweaterNumber")
        except Exception as e:
            logger.warning(f"Error fetching roster for {team_abbr}: {e}")
            continue

    logger.info(f"Got jersey numbers for {len(jersey_map)} players")
    return jersey_map


def fetch_traditional_stats(client: NHLClient) -> dict:
    """
    Fetch traditional stats for all league skaters.

    Returns:
        Dict mapping player_id to stats dict
    """
    logger.info("Fetching traditional stats for all skaters...")
    season = get_current_season()

    filters = [
        SeasonQuery(season_start=season, season_end=season),
        GameTypeQuery(game_type="2"),
    ]
    qb = QueryBuilder()
    query_ctx = qb.build(filters=filters)

    try:
        # Paginate through all summary stats
        all_summary = []
        for start in range(0, 1500, 100):
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
        logger.info(f"Total summary stats: {len(summary_data)} players")

        # Paginate through realtime stats (hits, blocks, etc.)
        all_realtime = []
        for start in range(0, 1500, 100):
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
    for player_id, summary in summary_data.items():
        realtime = realtime_data.get(player_id, {})

        # TOI is in seconds, convert to minutes
        toi_seconds = summary.get("timeOnIcePerGame", 0)
        avg_toi = toi_seconds / 60 if toi_seconds else None

        # Calculate shots per 60 and P/60
        games_played = summary.get("gamesPlayed", 0)
        shots = summary.get("shots", 0)
        points = summary.get("points", 0)
        shots_per_60 = None
        p60 = None
        toi_per_game = avg_toi  # Already in minutes

        if games_played > 0 and avg_toi and avg_toi > 0:
            minutes_played = games_played * avg_toi
            if minutes_played > 0:
                shots_per_60 = round((shots / minutes_played) * 60, 2)
                p60 = round((points / minutes_played) * 60, 2)

        stats[player_id] = {
            "games_played": games_played,
            "avg_toi": avg_toi,
            "goals": summary.get("goals"),
            "assists": summary.get("assists"),
            "points": points,
            "plus_minus": summary.get("plusMinus"),
            "hits": realtime.get("hits"),
            "pim": summary.get("penaltyMinutes"),
            "faceoff_win_pct": summary.get("faceoffWinPct"),
            "shots": shots,
            "shots_per_60": shots_per_60,
            "p60": p60,
            "toi_per_game": toi_per_game
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


def refresh_data():
    """
    Main refresh function - fetches all league data and updates database.

    Returns:
        Number of players updated
    """
    logger.info("Starting full league data refresh...")
    client = NHLClient()

    # 1. Get all league skaters with 10+ games
    all_skaters = fetch_all_league_skaters(client)
    if not all_skaters:
        logger.error("Failed to fetch league skaters")
        return 0

    # 2. Get traditional stats for all players
    trad_stats = fetch_traditional_stats(client)

    # 3. Get unique teams and fetch jersey numbers
    unique_teams = list(set(p["team_abbr"] for p in all_skaters if p["team_abbr"]))
    jersey_map = fetch_team_rosters(client, unique_teams)

    # 4. Collect all P/60 and TOI values for percentile calculation
    all_p60 = []
    forward_toi = []
    defensemen_toi = []

    for player in all_skaters:
        player_id = player["player_id"]
        if player_id in trad_stats:
            trad = trad_stats[player_id]
            if trad.get("p60") is not None:
                all_p60.append(trad["p60"])
            if trad.get("toi_per_game") is not None:
                if player["position"] in ['C', 'L', 'R']:
                    forward_toi.append(trad["toi_per_game"])
                elif player["position"] == 'D':
                    defensemen_toi.append(trad["toi_per_game"])

    # Sort for percentile calculation
    all_p60.sort()
    forward_toi.sort()
    defensemen_toi.sort()

    logger.info(f"P/60 samples: {len(all_p60)}, Forward TOI samples: {len(forward_toi)}, D TOI samples: {len(defensemen_toi)}")

    # 5. Save all players to database with percentiles
    logger.info(f"Saving {len(all_skaters)} players to database...")
    for player in all_skaters:
        player_id = player["player_id"]
        # Get jersey number from roster data
        jersey_number = jersey_map.get(player_id, player.get("jersey_number"))

        database.upsert_player(
            player_id=player_id,
            name=player["name"],
            position=player["position"],
            jersey_number=jersey_number,
            team_abbr=player.get("team_abbr")
        )

        # Calculate and add percentiles to traditional stats
        if player_id in trad_stats:
            trad = trad_stats[player_id]

            # P/60 percentile (all skaters)
            if trad.get("p60") is not None:
                trad["p60_percentile"] = calculate_percentile(trad["p60"], all_p60)

            # TOI/G percentile (by position)
            if trad.get("toi_per_game") is not None:
                if player["position"] in ['C', 'L', 'R']:
                    trad["toi_per_game_percentile"] = calculate_percentile(trad["toi_per_game"], forward_toi)
                elif player["position"] == 'D':
                    trad["toi_per_game_percentile"] = calculate_percentile(trad["toi_per_game"], defensemen_toi)

            database.upsert_player_stats(player_id, trad)

    # 6. Fetch Edge stats for ALL qualified players
    logger.info(f"Fetching Edge stats for {len(all_skaters)} players (this will take a while)...")
    edge_stats = {}

    for i, player in enumerate(all_skaters):
        player_id = player["player_id"]
        edge = fetch_edge_stats(client, player_id)
        if edge:
            edge_stats[player_id] = edge

        if (i + 1) % 50 == 0:
            logger.info(f"Fetched Edge stats: {i + 1}/{len(all_skaters)} players")

    logger.info(f"Collected Edge stats for {len(edge_stats)} players")

    # 7. Collect values for percentile calculation
    all_shots_per_60 = []
    all_distance_per_game = []
    for player in all_skaters:
        player_id = player["player_id"]
        if player_id in trad_stats:
            shots_per_60 = trad_stats[player_id].get("shots_per_60")
            if shots_per_60 is not None:
                all_shots_per_60.append(shots_per_60)
        if player_id in edge_stats:
            dist_per_game = edge_stats[player_id].get("distance_per_game_miles")
            if dist_per_game is not None:
                all_distance_per_game.append(dist_per_game)
    all_shots_per_60.sort()
    all_distance_per_game.sort()

    logger.info(f"Shots/60 samples: {len(all_shots_per_60)}, Dist/G samples: {len(all_distance_per_game)}")

    # 8. Save Edge stats with calculated percentiles
    players_updated = 0
    for player in all_skaters:
        player_id = player["player_id"]

        if player_id in edge_stats:
            edge = edge_stats[player_id]
            trad = trad_stats.get(player_id, {})

            # Calculate shots percentile
            shots_per_60 = trad.get("shots_per_60")
            if shots_per_60 is not None and all_shots_per_60:
                edge["shots_percentile"] = calculate_percentile(shots_per_60, all_shots_per_60)

            # Calculate distance per game percentile (override NHL's total distance percentile)
            dist_per_game = edge.get("distance_per_game_miles")
            if dist_per_game is not None and all_distance_per_game:
                edge["distance_percentile"] = calculate_percentile(dist_per_game, all_distance_per_game)

            database.upsert_player_edge_stats(player_id, edge)
            players_updated += 1

    # Update timestamp
    database.set_last_updated(datetime.now())
    logger.info(f"Data refresh complete. Updated {players_updated} players with Edge stats.")

    return players_updated


if __name__ == "__main__":
    # Allow running directly for testing
    refresh_data()
