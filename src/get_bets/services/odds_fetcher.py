import sqlite3
from pathlib import Path

from src.get_bets.models.bet import BettingLine


class OddsFetcher:
    """Busca odds e informações de eventos do lol_odds.db"""
    
    def __init__(self, db_path: str = "data/lol_odds.db"):
        self.db_path = Path(db_path)
    
    def get_available_events(self) -> list[str]:
        """Retorna IDs de eventos com odds de Totals disponíveis"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT DISTINCT event_id
                FROM current_odds
                WHERE market_type LIKE '%Totals'
                AND odds_type IN ('map_1', 'map_2')
                """
            )
            return [row[0] for row in cursor.fetchall()]
    
    def get_event_info(self, event_id: str) -> dict | None:
        """Busca informações do evento (times, liga, data)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Buscar evento
            cursor = conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (event_id,)
            )
            event = cursor.fetchone()
            
            if not event:
                return None
            
            # Buscar nomes dos times
            home_cursor = conn.execute(
                "SELECT name FROM teams WHERE id = ?",
                (event["home_team_id"],)
            )
            home_team = home_cursor.fetchone()
            
            away_cursor = conn.execute(
                "SELECT name FROM teams WHERE id = ?",
                (event["away_team_id"],)
            )
            away_team = away_cursor.fetchone()
            
            # CORREÇÃO: Usar indexação por chave [] em vez de .get() para sqlite3.Row
            return {
                "event_id": event["event_id"],
                "FI": event["FI"],
                "bet365_key": event["bet365_key"],
                "home_team_id": event["home_team_id"],
                "away_team_id": event["away_team_id"],
                "home_team_name": home_team[0] if home_team else "Unknown",
                "away_team_name": away_team[0] if away_team else "Unknown",
                "league_name": event["league_name"],
                "match_date": event["match_date"],
                "match_timestamp": event["match_timestamp"],
            }
    
    def get_betting_lines(self, event_id: str, market_types: list[str] = None) -> list[BettingLine]:
        """Busca linhas de apostas disponíveis para um evento"""
        if market_types is None:
            market_types = ["Map 1 - Totals", "Map 2 - Totals"]
        
        betting_lines = []
        
        with sqlite3.connect(self.db_path) as conn:
            for market_type in market_types:
                # Determinar odds_type baseado no market
                if "Map 1" in market_type:
                    odds_type = "map_1"
                    map_number = 1
                elif "Map 2" in market_type:
                    odds_type = "map_2"
                    map_number = 2
                else:
                    odds_type = "main"
                    map_number = None
                
                cursor = conn.execute(
                    """
                    SELECT selection, odds, line
                    FROM current_odds
                    WHERE event_id = ?
                    AND market_type = ?
                    AND odds_type = ?
                    ORDER BY selection, line
                    """,
                    (event_id, market_type, odds_type)
                )
                
                for row in cursor.fetchall():
                    selection, odds, line = row
                    betting_lines.append(
                        BettingLine(
                            event_id=event_id,
                            market_type=market_type,
                            selection=selection,
                            odds=float(odds),
                            line=str(line) if line else "0",
                            map_number=map_number,
                            odds_type=odds_type,
                        )
                    )
        
        return betting_lines
