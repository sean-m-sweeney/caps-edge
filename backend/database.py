"""Database setup and queries for Caps Edge."""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional
import os

# Database path - use /app/data in Docker, local data/ otherwise
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).parent.parent / "data"))
DB_PATH = DATA_DIR / "caps_edge.db"


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database schema."""
    conn = get_connection()
    cursor = conn.cursor()

    # Players table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            position TEXT NOT NULL,
            jersey_number INTEGER
        )
    """)

    # Player stats table (traditional stats)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            updated_at DATETIME NOT NULL,
            games_played INTEGER,
            avg_toi REAL,
            goals INTEGER,
            assists INTEGER,
            points INTEGER,
            plus_minus INTEGER,
            hits INTEGER,
            pim INTEGER,
            faceoff_win_pct REAL,
            shots INTEGER,
            shots_per_60 REAL,
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        )
    """)

    # Player Edge stats table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_edge_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            updated_at DATETIME NOT NULL,

            -- Skating Speed
            top_speed_mph REAL,
            top_speed_percentile INTEGER,

            -- Bursts
            bursts_20_plus INTEGER,
            bursts_20_percentile INTEGER,
            bursts_22_plus INTEGER,
            bursts_22_percentile INTEGER,

            -- Distance
            distance_per_game_miles REAL,
            distance_percentile INTEGER,

            -- Zone Time
            off_zone_time_pct REAL,
            off_zone_percentile INTEGER,
            def_zone_time_pct REAL,
            def_zone_percentile INTEGER,
            neu_zone_time_pct REAL,

            -- Zone Starts
            zone_starts_off_pct REAL,
            zone_starts_percentile INTEGER,

            -- Shot Speed
            top_shot_speed_mph REAL,
            shot_speed_percentile INTEGER,

            -- Shots percentile (for shots/60)
            shots_percentile INTEGER,

            -- Motor Index (replaces Hustle Score)
            motor_index REAL,
            motor_percentile INTEGER,

            FOREIGN KEY (player_id) REFERENCES players(player_id)
        )
    """)

    # Metadata table for tracking updates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Position averages table (rebuilt on each refresh)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS position_averages (
            position TEXT PRIMARY KEY,
            avg_bursts_per_60 REAL,
            avg_distance_per_game REAL,
            avg_hits_per_60 REAL,
            avg_shots_per_60 REAL,
            avg_off_zone_pct REAL,
            sample_size INTEGER
        )
    """)

    # League stats for Motor Index percentile calculations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS league_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at DATETIME NOT NULL,
            player_id INTEGER NOT NULL,
            position TEXT,
            games_played INTEGER,
            avg_toi REAL,
            hits INTEGER,
            shots INTEGER,
            bursts_20_plus INTEGER,
            distance_per_game_miles REAL,
            off_zone_time_pct REAL,
            motor_index REAL
        )
    """)

    # Run migrations for existing databases
    _run_migrations(cursor)

    conn.commit()
    conn.close()


def _run_migrations(cursor):
    """Run database migrations for schema changes."""
    # Check if we need to add new columns to player_stats
    cursor.execute("PRAGMA table_info(player_stats)")
    columns = [col[1] for col in cursor.fetchall()]

    if "shots" not in columns:
        cursor.execute("ALTER TABLE player_stats ADD COLUMN shots INTEGER")
    if "shots_per_60" not in columns:
        cursor.execute("ALTER TABLE player_stats ADD COLUMN shots_per_60 REAL")

    # Check player_edge_stats for motor columns
    cursor.execute("PRAGMA table_info(player_edge_stats)")
    edge_columns = [col[1] for col in cursor.fetchall()]

    if "motor_index" not in edge_columns:
        cursor.execute("ALTER TABLE player_edge_stats ADD COLUMN motor_index REAL")
    if "motor_percentile" not in edge_columns:
        cursor.execute("ALTER TABLE player_edge_stats ADD COLUMN motor_percentile INTEGER")
    if "shots_percentile" not in edge_columns:
        cursor.execute("ALTER TABLE player_edge_stats ADD COLUMN shots_percentile INTEGER")

    # Check league_stats for position and shots
    cursor.execute("PRAGMA table_info(league_stats)")
    league_columns = [col[1] for col in cursor.fetchall()]

    if "position" not in league_columns:
        cursor.execute("ALTER TABLE league_stats ADD COLUMN position TEXT")
    if "shots" not in league_columns:
        cursor.execute("ALTER TABLE league_stats ADD COLUMN shots INTEGER")
    if "motor_index" not in league_columns:
        cursor.execute("ALTER TABLE league_stats ADD COLUMN motor_index REAL")


def get_last_updated() -> Optional[datetime]:
    """Get the last update timestamp."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM metadata WHERE key = 'last_updated'")
    row = cursor.fetchone()
    conn.close()
    if row:
        return datetime.fromisoformat(row["value"])
    return None


def set_last_updated(timestamp: datetime):
    """Set the last update timestamp."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_updated', ?)
    """, (timestamp.isoformat(),))
    conn.commit()
    conn.close()


def upsert_player(player_id: int, name: str, position: str, jersey_number: Optional[int]):
    """Insert or update a player."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO players (player_id, name, position, jersey_number)
        VALUES (?, ?, ?, ?)
    """, (player_id, name, position, jersey_number))
    conn.commit()
    conn.close()


def upsert_player_stats(player_id: int, stats: dict):
    """Insert or update player stats (keeps only latest)."""
    conn = get_connection()
    cursor = conn.cursor()

    # Delete old stats for this player
    cursor.execute("DELETE FROM player_stats WHERE player_id = ?", (player_id,))

    # Insert new stats
    cursor.execute("""
        INSERT INTO player_stats (
            player_id, updated_at, games_played, avg_toi, goals, assists,
            points, plus_minus, hits, pim, faceoff_win_pct, shots, shots_per_60
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        player_id,
        datetime.now().isoformat(),
        stats.get("games_played"),
        stats.get("avg_toi"),
        stats.get("goals"),
        stats.get("assists"),
        stats.get("points"),
        stats.get("plus_minus"),
        stats.get("hits"),
        stats.get("pim"),
        stats.get("faceoff_win_pct"),
        stats.get("shots"),
        stats.get("shots_per_60")
    ))
    conn.commit()
    conn.close()


def upsert_player_edge_stats(player_id: int, stats: dict):
    """Insert or update player Edge stats (keeps only latest)."""
    conn = get_connection()
    cursor = conn.cursor()

    # Delete old stats for this player
    cursor.execute("DELETE FROM player_edge_stats WHERE player_id = ?", (player_id,))

    # Insert new stats
    cursor.execute("""
        INSERT INTO player_edge_stats (
            player_id, updated_at,
            top_speed_mph, top_speed_percentile,
            bursts_20_plus, bursts_20_percentile,
            bursts_22_plus, bursts_22_percentile,
            distance_per_game_miles, distance_percentile,
            off_zone_time_pct, off_zone_percentile,
            def_zone_time_pct, def_zone_percentile,
            neu_zone_time_pct,
            zone_starts_off_pct, zone_starts_percentile,
            top_shot_speed_mph, shot_speed_percentile,
            shots_percentile,
            motor_index, motor_percentile
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        player_id,
        datetime.now().isoformat(),
        stats.get("top_speed_mph"),
        stats.get("top_speed_percentile"),
        stats.get("bursts_20_plus"),
        stats.get("bursts_20_percentile"),
        stats.get("bursts_22_plus"),
        stats.get("bursts_22_percentile"),
        stats.get("distance_per_game_miles"),
        stats.get("distance_percentile"),
        stats.get("off_zone_time_pct"),
        stats.get("off_zone_percentile"),
        stats.get("def_zone_time_pct"),
        stats.get("def_zone_percentile"),
        stats.get("neu_zone_time_pct"),
        stats.get("zone_starts_off_pct"),
        stats.get("zone_starts_percentile"),
        stats.get("top_shot_speed_mph"),
        stats.get("shot_speed_percentile"),
        stats.get("shots_percentile"),
        stats.get("motor_index"),
        stats.get("motor_percentile")
    ))
    conn.commit()
    conn.close()


def clear_league_stats():
    """Clear league stats table for fresh calculation."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM league_stats")
    conn.commit()
    conn.close()


def clear_position_averages():
    """Clear position averages table for fresh calculation."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM position_averages")
    conn.commit()
    conn.close()


def insert_position_averages(position: str, avgs: dict):
    """Insert position averages."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO position_averages (
            position, avg_bursts_per_60, avg_distance_per_game,
            avg_hits_per_60, avg_shots_per_60, avg_off_zone_pct, sample_size
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        position,
        avgs.get("avg_bursts_per_60"),
        avgs.get("avg_distance_per_game"),
        avgs.get("avg_hits_per_60"),
        avgs.get("avg_shots_per_60"),
        avgs.get("avg_off_zone_pct"),
        avgs.get("sample_size")
    ))
    conn.commit()
    conn.close()


def get_position_averages() -> dict:
    """Get position averages as a dict keyed by position."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM position_averages")
    rows = cursor.fetchall()
    conn.close()
    return {row["position"]: dict(row) for row in rows}


def insert_league_stats(player_id: int, stats: dict):
    """Insert league-wide player stats for percentile calculation."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO league_stats (
            updated_at, player_id, position, games_played, avg_toi, hits, shots,
            bursts_20_plus, distance_per_game_miles, off_zone_time_pct, motor_index
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().isoformat(),
        player_id,
        stats.get("position"),
        stats.get("games_played"),
        stats.get("avg_toi"),
        stats.get("hits"),
        stats.get("shots"),
        stats.get("bursts_20_plus"),
        stats.get("distance_per_game_miles"),
        stats.get("off_zone_time_pct"),
        stats.get("motor_index")
    ))
    conn.commit()
    conn.close()


def get_league_motor_scores() -> list:
    """Get all motor index scores from league stats for percentile calculation."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT motor_index FROM league_stats
        WHERE games_played >= 10 AND motor_index IS NOT NULL
        ORDER BY motor_index
    """)
    rows = cursor.fetchall()
    conn.close()
    return [row["motor_index"] for row in rows]


def get_all_players_with_stats() -> list:
    """Get all players with their stats and edge stats."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            p.player_id, p.name, p.position, p.jersey_number,
            s.games_played, s.avg_toi, s.goals, s.assists, s.points,
            s.plus_minus, s.hits, s.pim, s.faceoff_win_pct,
            s.shots, s.shots_per_60,
            e.top_speed_mph, e.top_speed_percentile,
            e.bursts_20_plus, e.bursts_20_percentile,
            e.bursts_22_plus, e.bursts_22_percentile,
            e.distance_per_game_miles, e.distance_percentile,
            e.off_zone_time_pct, e.off_zone_percentile,
            e.def_zone_time_pct, e.def_zone_percentile,
            e.neu_zone_time_pct,
            e.zone_starts_off_pct, e.zone_starts_percentile,
            e.top_shot_speed_mph, e.shot_speed_percentile,
            e.shots_percentile,
            e.motor_index, e.motor_percentile
        FROM players p
        LEFT JOIN player_stats s ON p.player_id = s.player_id
        LEFT JOIN player_edge_stats e ON p.player_id = e.player_id
        WHERE p.position != 'G'
        ORDER BY s.points DESC NULLS LAST
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_player_by_id(player_id: int) -> Optional[dict]:
    """Get a single player with all stats."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            p.player_id, p.name, p.position, p.jersey_number,
            s.games_played, s.avg_toi, s.goals, s.assists, s.points,
            s.plus_minus, s.hits, s.pim, s.faceoff_win_pct,
            s.shots, s.shots_per_60,
            e.top_speed_mph, e.top_speed_percentile,
            e.bursts_20_plus, e.bursts_20_percentile,
            e.bursts_22_plus, e.bursts_22_percentile,
            e.distance_per_game_miles, e.distance_percentile,
            e.off_zone_time_pct, e.off_zone_percentile,
            e.def_zone_time_pct, e.def_zone_percentile,
            e.neu_zone_time_pct,
            e.zone_starts_off_pct, e.zone_starts_percentile,
            e.top_shot_speed_mph, e.shot_speed_percentile,
            e.shots_percentile,
            e.motor_index, e.motor_percentile
        FROM players p
        LEFT JOIN player_stats s ON p.player_id = s.player_id
        LEFT JOIN player_edge_stats e ON p.player_id = e.player_id
        WHERE p.player_id = ?
    """, (player_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


# Initialize database on import
init_db()
