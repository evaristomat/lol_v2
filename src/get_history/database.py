import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


class HistoryDatabase:
    def __init__(self, db_path: str = "data/lol_history.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()

    def _init_tables(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leagues (
                    id INTEGER PRIMARY KEY,
                    league_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    created_at TEXT
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY,
                    team_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    image_id TEXT,
                    country_code TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY,
                    bet365_id TEXT NOT NULL UNIQUE,
                    sport_id TEXT,
                    league_id TEXT,
                    home_team_id TEXT NOT NULL,
                    away_team_id TEXT NOT NULL,
                    event_time TEXT,
                    time_status INTEGER,
                    final_score TEXT,
                    retrieved_at TEXT,
                    updated_at TEXT
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS game_maps (
                    id INTEGER PRIMARY KEY,
                    match_id INTEGER NOT NULL,
                    map_number INTEGER NOT NULL,
                    created_at TEXT,
                    FOREIGN KEY (match_id) REFERENCES matches(id)
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS map_statistics (
                    id INTEGER PRIMARY KEY,
                    map_id INTEGER NOT NULL,
                    stat_name TEXT NOT NULL,
                    home_value TEXT,
                    away_value TEXT,
                    created_at TEXT,
                    FOREIGN KEY (map_id) REFERENCES game_maps(id)
                )
            """
            )

    def insert_league(self, league_id: str, name: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO leagues (league_id, name, created_at) VALUES (?, ?, ?)",
                (league_id, name, datetime.now().isoformat()),
            )

    def insert_team(self, team_data: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO teams (team_id, name, image_id, country_code, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    team_data["team_id"],
                    team_data["name"],
                    team_data.get("image_id"),
                    team_data.get("country_code"),
                    datetime.now().isoformat(),
                ),
            )

    def get_match(self, bet365_id: str) -> dict | None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM matches WHERE bet365_id = ?", (bet365_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def insert_match(self, match_data: dict) -> int:
        now = datetime.now().isoformat()
        match_data["retrieved_at"] = now
        match_data["updated_at"] = now

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO matches (
                    bet365_id, sport_id, league_id, home_team_id, away_team_id,
                    event_time, time_status, final_score, retrieved_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    match_data["bet365_id"],
                    match_data.get("sport_id"),
                    match_data.get("league_id"),
                    match_data["home_team_id"],
                    match_data["away_team_id"],
                    match_data.get("event_time"),
                    match_data.get("time_status"),
                    match_data.get("final_score"),
                    match_data["retrieved_at"],
                    match_data["updated_at"],
                ),
            )
            return cursor.lastrowid

    def update_match(self, bet365_id: str, match_data: dict):
        match_data["updated_at"] = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE matches SET
                    final_score = ?,
                    time_status = ?,
                    updated_at = ?
                WHERE bet365_id = ?
                """,
                (
                    match_data.get("final_score"),
                    match_data.get("time_status"),
                    match_data["updated_at"],
                    bet365_id,
                ),
            )

    def insert_game_map(self, match_id: int, map_number: int) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO game_maps (match_id, map_number, created_at)
                VALUES (?, ?, ?)
                """,
                (match_id, map_number, datetime.now().isoformat()),
            )
            return cursor.lastrowid

    def insert_map_statistic(self, stat_data: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO map_statistics (
                    map_id, stat_name, home_value, away_value, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    stat_data["map_id"],
                    stat_data["stat_name"],
                    stat_data.get("home_value"),
                    stat_data.get("away_value"),
                    datetime.now().isoformat(),
                ),
            )

    def get_incomplete_matches(self, days_back: int = 7) -> list[dict]:
        """Busca partidas com status != 3 OU sem final_score dos últimos N dias"""
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM matches 
                WHERE (time_status != 3 OR final_score IS NULL)
                AND event_time >= ?
                ORDER BY event_time DESC
                """,
                (cutoff_date,),
            )
            return [dict(row) for row in cursor.fetchall()]
