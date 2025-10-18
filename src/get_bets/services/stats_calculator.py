import sqlite3
from pathlib import Path

from src.get_bets.models.bet import TeamStats


class StatsCalculator:
    """Calcula estatísticas históricas dos times do lol_history.db"""
    
    def __init__(self, db_path: str = "data/lol_history.db"):
        self.db_path = Path(db_path)
    
    def get_team_stats(self, team_name: str, stat_type: str, limit: int = 20) -> TeamStats:
        """
        Busca estatísticas históricas de um time.
        
        stat_type: 'dragons', 'barons', 'kills', 'towers', 'inhibitors'
        """
        with sqlite3.connect(self.db_path) as conn:
            # 1. Buscar team_id pelo nome
            cursor = conn.execute(
                "SELECT team_id FROM teams WHERE name = ?",
                (team_name,)
            )
            team_result = cursor.fetchone()
            
            if not team_result:
                return TeamStats(team_name, stat_type, [])
            
            team_id = team_result[0]
            
            # 2. Buscar últimas partidas do time
            cursor = conn.execute(
                """
                SELECT id, home_team_id, away_team_id
                FROM matches
                WHERE (home_team_id = ? OR away_team_id = ?)
                AND time_status = 3
                AND final_score IS NOT NULL
                ORDER BY event_time DESC
                LIMIT ?
                """,
                (team_id, team_id, limit * 3)
            )
            matches = cursor.fetchall()
            
            if not matches:
                return TeamStats(team_name, stat_type, [])
            
            # 3. Buscar estatísticas dos mapas
            match_ids = [str(m[0]) for m in matches]
            placeholders = ",".join(["?"] * len(match_ids))
            
            cursor = conn.execute(
                f"""
                SELECT gm.id, gm.match_id
                FROM game_maps gm
                WHERE gm.match_id IN ({placeholders})
                ORDER BY gm.match_id DESC, gm.map_number ASC
                """,
                match_ids
            )
            game_maps = cursor.fetchall()
            
            if not game_maps:
                return TeamStats(team_name, stat_type, [])
            
            # 4. Buscar estatísticas específicas
            map_ids = [str(gm[0]) for gm in game_maps]
            placeholders = ",".join(["?"] * len(map_ids))
            
            cursor = conn.execute(
                f"""
                SELECT map_id, home_value, away_value
                FROM map_statistics
                WHERE map_id IN ({placeholders})
                AND stat_name = ?
                ORDER BY map_id DESC
                """,
                map_ids + [stat_type]
            )
            
            # 5. Processar valores
            values = []
            for row in cursor.fetchall():
                map_id, home_value, away_value = row
                try:
                    home_val = float(home_value) if home_value else 0.0
                    away_val = float(away_value) if away_value else 0.0
                    total = home_val + away_val
                    
                    if stat_type == "inhibitors" and total == 0:
                        continue
                    
                    values.append(total)
                    
                    if len(values) >= limit:
                        break
                except (ValueError, TypeError):
                    continue
            
            return TeamStats(team_name, stat_type, values[:limit])