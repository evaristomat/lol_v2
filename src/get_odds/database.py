from src.shared.core.database import BaseDatabase


class OddsDatabase(BaseDatabase):
    """Banco de dados específico para odds"""

    def __init__(self):
        super().__init__("lol_odds.db")
        self._init_schema()

    def _init_schema(self):
        with self.get_connection() as conn:
            # Verificar se precisa adicionar colunas
            cursor = conn.execute("PRAGMA table_info(events)")
            columns = {row[1] for row in cursor.fetchall()}

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY,
                    team_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    region TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY,
                    event_id TEXT UNIQUE NOT NULL,
                    FI TEXT,
                    bet365_key TEXT,
                    home_team_id INTEGER NOT NULL,
                    away_team_id INTEGER NOT NULL,
                    league_name TEXT NOT NULL,
                    match_date TEXT,
                    match_timestamp INTEGER,
                    status TEXT DEFAULT 'upcoming',
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (home_team_id) REFERENCES teams (id),
                    FOREIGN KEY (away_team_id) REFERENCES teams (id)
                )
            """
            )

            # Adicionar colunas se não existirem
            if "FI" not in columns and columns:
                conn.execute("ALTER TABLE events ADD COLUMN FI TEXT")
            if "bet365_key" not in columns and columns:
                conn.execute("ALTER TABLE events ADD COLUMN bet365_key TEXT")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS current_odds (
                    id INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    odds_type TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    selection TEXT NOT NULL,
                    odds REAL NOT NULL,
                    line TEXT,
                    map_number INTEGER,
                    updated_at TEXT DEFAULT (datetime('now')),
                    raw_data TEXT,
                    FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                )
            """
            )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_teams_team_id ON teams (team_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (match_timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_status ON events (status)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_FI ON events (FI)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_odds_event ON current_odds (event_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_odds_market ON current_odds (market_type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_odds_map ON current_odds (map_number)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_odds_updated ON current_odds (updated_at)"
            )
