import sqlite3
from datetime import datetime
from pathlib import Path

from src.get_bets.models.bet import Bet, Event


class BetsDatabase:
    def __init__(self, db_path: str = "data/lol_bets.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.setup_database()  # Criar tabelas ao inicializar

    def setup_database(self):
        """Cria as tabelas necessárias se não existirem"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Tabela de times (mantém igual)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # Tabela de eventos (mantém igual)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE NOT NULL,
                    FI TEXT,
                    bet365_key TEXT,
                    home_team_id INTEGER NOT NULL,
                    away_team_id INTEGER NOT NULL,
                    league_name TEXT NOT NULL,
                    match_date TEXT,
                    match_timestamp TEXT,
                    status TEXT DEFAULT 'upcoming',
                    home_score INTEGER DEFAULT 0,
                    away_score INTEGER DEFAULT 0,
                    winner TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (home_team_id) REFERENCES teams (id),
                    FOREIGN KEY (away_team_id) REFERENCES teams (id)
                )
            """)

            # Tabela de apostas - ✅ COM STRATEGY
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    odds_type TEXT DEFAULT 'main',
                    market_type TEXT NOT NULL,
                    selection TEXT NOT NULL,
                    odds REAL NOT NULL,
                    line TEXT NOT NULL,
                    map_number INTEGER,
                    roi_average REAL NOT NULL,
                    fair_odds REAL NOT NULL,
                    stake REAL DEFAULT 1.0,
                    potential_win REAL DEFAULT 0.0,
                    bet_status TEXT DEFAULT 'pending',
                    actual_value REAL,
                    actual_win REAL DEFAULT 0.0,
                    result_verified BOOLEAN DEFAULT FALSE,
                    strategy TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                )
            """)

            # Índices para melhor performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bets_event_id ON bets(event_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(bet_status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_teams_team_id ON teams(team_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bets_strategy ON bets(strategy)")  # ✅ NOVO

            conn.commit()

    # ... (seus métodos existentes continuam aqui)
    def get_event(self, event_id: str) -> dict | None:
        """Busca um evento por ID"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (event_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def insert_event(self, event: Event) -> int:
        """Insere um novo evento"""
        event_data = event.to_dict()
        event_data["created_at"] = datetime.now().isoformat()
        event_data["updated_at"] = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO events (
                    event_id, FI, bet365_key, home_team_id, away_team_id,
                    league_name, match_date, match_timestamp, status,
                    home_score, away_score, winner, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, ?, ?)
                """,
                (
                    event_data["event_id"],
                    event_data.get("FI"),
                    event_data.get("bet365_key"),
                    event_data["home_team_id"],
                    event_data["away_team_id"],
                    event_data["league_name"],
                    event_data.get("match_date"),
                    event_data.get("match_timestamp"),
                    event_data.get("status", "upcoming"),
                    event_data["created_at"],
                    event_data["updated_at"],
                ),
            )
            return cursor.lastrowid

    def get_or_create_team(self, team_id: str, team_name: str) -> int:
        """Busca ou cria um time, retorna o ID interno"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT id FROM teams WHERE team_id = ?",
                (team_id,)
            )
            row = cursor.fetchone()
            
            if row:
                return row[0]
            
            # Criar novo time
            cursor = conn.execute(
                """
                INSERT INTO teams (team_id, name, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (team_id, team_name, datetime.now().isoformat(), datetime.now().isoformat())
            )
            return cursor.lastrowid

    def insert_bet(self, bet) -> int:
        """Insere uma nova aposta - aceita objeto Bet ou dicionário"""
        # Converter para dicionário se for objeto Bet
        if hasattr(bet, 'to_dict'):
            bet_data = bet.to_dict()
        else:
            bet_data = bet  # Já é um dicionário
        
        bet_data["created_at"] = datetime.now().isoformat()
        bet_data["updated_at"] = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO bets (
                    event_id, odds_type, market_type, selection, odds, line,
                    map_number, roi_average, fair_odds, stake, potential_win,
                    bet_status, actual_value, actual_win, result_verified,
                    strategy, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, 0, FALSE, ?, ?, ?)
                """,
                (
                    bet_data["event_id"],
                    bet_data.get("odds_type", "main"),
                    bet_data["market_type"],
                    bet_data["selection"],
                    bet_data["odds"],
                    bet_data["line"],
                    bet_data.get("map_number"),
                    bet_data["roi_average"],
                    bet_data["fair_odds"],
                    bet_data.get("stake", 1.0),
                    bet_data.get("potential_win", 0.0),
                    bet_data.get("strategy", "unknown"),  # ✅ ADICIONAR
                    bet_data["created_at"],
                    bet_data["updated_at"],
                ),
            )
            return cursor.lastrowid

    def bet_exists(self, event_id: str, market_name: str, selection_line: str, handicap: float) -> bool:
        """Verifica se uma aposta já existe - formato da estratégia"""
        # Converter para formato do banco
        market_type = market_name  # No banco é market_type
        selection = selection_line  # No banco é selection
        line = str(handicap)       # No banco line é TEXT
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT id FROM bets
                WHERE event_id = ? AND market_type = ? AND selection = ? AND line = ?
                """,
                (event_id, market_type, selection, line)
            )
            return cursor.fetchone() is not None

    def get_analyzed_events(self) -> set[str]:
        """Retorna IDs dos eventos já analisados"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT DISTINCT event_id FROM events")
            return {row[0] for row in cursor.fetchall()}

    def get_stats(self) -> dict:
        """Retorna estatísticas gerais"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT 
                    COUNT(*) as total_bets,
                    SUM(CASE WHEN bet_status = 'won' THEN 1 ELSE 0 END) as won_bets,
                    SUM(CASE WHEN bet_status = 'lost' THEN 1 ELSE 0 END) as lost_bets,
                    SUM(CASE WHEN bet_status = 'pending' THEN 1 ELSE 0 END) as pending_bets,
                    SUM(actual_win) as total_profit_loss,
                    AVG(CASE WHEN bet_status != 'pending' THEN odds ELSE NULL END) as avg_odds,
                    SUM(stake) as total_stake
                FROM bets
                """
            )
            row = cursor.fetchone()
            
            if not row or row[0] == 0:
                return {}
            
            total_bets, won_bets, lost_bets, pending_bets, total_profit_loss, avg_odds, total_stake = row
            
            stats = {
                "total_bets": total_bets,
                "won_bets": won_bets or 0,
                "lost_bets": lost_bets or 0,
                "pending_bets": pending_bets or 0,
                "total_profit_loss": total_profit_loss or 0,
                "avg_odds": avg_odds or 0,
                "total_stake": total_stake or 0,
            }
            
            # Calcular ROI
            if stats["total_stake"] > 0:
                stats["roi"] = (stats["total_profit_loss"] / stats["total_stake"]) * 100
            else:
                stats["roi"] = 0
            
            # Calcular win rate
            finished_bets = stats["won_bets"] + stats["lost_bets"]
            if finished_bets > 0:
                stats["win_rate"] = (stats["won_bets"] / finished_bets) * 100
            else:
                stats["win_rate"] = 0
            
            return stats
        
        # No database.py, adicionar este método:
    def insert_bet_from_dict(self, bet_dict: dict) -> int:
        """Insere uma aposta a partir de um dicionário (formato da estratégia)"""
        try:
            # Converter formato da estratégia para formato do banco
            bet_data = {
                'event_id': bet_dict['event_id'],
                'market_type': bet_dict['market_name'],  # Converter market_name para market_type
                'selection': bet_dict['selection_line'],  # Converter selection_line para selection
                'odds': bet_dict['house_odds'],
                'line': str(bet_dict['handicap']),  # line é TEXT no banco
                'roi_average': bet_dict['roi_average'],
                'fair_odds': bet_dict['fair_odds'],
                'stake': 1.0,  # Stake padrão
                'potential_win': (bet_dict['house_odds'] - 1) * 1.0,
                'odds_type': 'player',  # Tipo fixo para apostas de players
                'map_number': 1,  # Mapa 1 para player bets
                'actual_value': bet_dict.get('actual_value')
            }
            
            return self.insert_bet(bet_data)
        except Exception as e:
            print(f"❌ Erro ao converter aposta: {e}")
            return 0
        
    def get_stats_by_strategy(self) -> dict:
        """Retorna estatísticas agrupadas por estratégia"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT 
                    COALESCE(strategy, 'unknown') as strategy,
                    COUNT(*) as total_bets,
                    SUM(CASE WHEN bet_status = 'won' THEN 1 ELSE 0 END) as won_bets,
                    SUM(CASE WHEN bet_status = 'lost' THEN 1 ELSE 0 END) as lost_bets,
                    SUM(CASE WHEN bet_status = 'pending' THEN 1 ELSE 0 END) as pending_bets,
                    AVG(roi_average) as avg_roi,
                    SUM(stake) as total_stake,
                    SUM(CASE WHEN bet_status != 'pending' THEN actual_win ELSE 0 END) as total_profit
                FROM bets
                GROUP BY strategy
                """
            )
            
            results = {}
            for row in cursor.fetchall():
                strategy = row[0]
                won = row[2]
                lost = row[3]
                total_stake = row[6]
                total_profit = row[7]
                
                win_rate = (won / (won + lost) * 100) if (won + lost) > 0 else 0
                roi = (total_profit / total_stake * 100) if total_stake > 0 else 0
                
                results[strategy] = {
                    'total_bets': row[1],
                    'won_bets': won,
                    'lost_bets': lost,
                    'pending_bets': row[4],
                    'avg_roi': row[5] or 0,
                    'total_stake': total_stake or 0,
                    'total_profit': total_profit or 0,
                    'win_rate': win_rate,
                    'roi_percent': roi
                }
            
            return results