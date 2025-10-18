import logging
import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd
from colorama import Back, Fore, Style, init

logger = logging.getLogger("lol_bets")

class ROIAnalyzer:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        # Inicializa colorama
        init()

    def connect(self):
        """Conecta ao banco de dados"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            return True
        except sqlite3.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            return False

    def disconnect(self):
        """Desconecta do banco de dados"""
        if self.conn:
            self.conn.close()

    def get_market_odds(
        self, event_id: str, market_type: str = "Map 1 - Totals", odds_type: str = "map_1"
    ) -> pd.DataFrame:
        """Busca as odds de um mercado específico para um evento"""
        # ✅ Query atualizada com novos nomes de colunas
        query = """
        SELECT selection, line, odds, updated_at, market_type
        FROM current_odds
        WHERE event_id = ? 
        AND market_type = ?
        AND odds_type = ?
        ORDER BY selection, line
        """

        try:
            df = pd.read_sql_query(query, self.conn, params=[event_id, market_type, odds_type])
            # Renomear para compatibilidade com código antigo
            df = df.rename(columns={
                'selection': 'selection_name',
                'line': 'handicap',
                'odds': 'odds_value',
                'market_type': 'market_name'
            })
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar odds: {e}")
            return pd.DataFrame()

    def get_event_info(self, event_id: str) -> Dict:
        """Busca informações do evento (times, data, etc.)"""
        if not self.connect():
            return {}

        try:
            # Primeiro busca as informações do evento
            query = """
            SELECT home_team_id, away_team_id, league_name, match_date
            FROM events
            WHERE event_id = ?
            """
            cursor = self.conn.cursor()
            cursor.execute(query, (event_id,))
            result = cursor.fetchone()

            if result:
                home_team_id, away_team_id, league_name, match_date = result

                # Busca os nomes dos times
                home_team_query = "SELECT name FROM teams WHERE id = ?"
                cursor.execute(home_team_query, (home_team_id,))
                home_team_result = cursor.fetchone()
                home_team = (
                    home_team_result[0] if home_team_result else f"Team {home_team_id}"
                )

                away_team_query = "SELECT name FROM teams WHERE id = ?"
                cursor.execute(away_team_query, (away_team_id,))
                away_team_result = cursor.fetchone()
                away_team = (
                    away_team_result[0] if away_team_result else f"Team {away_team_id}"
                )

                return {
                    "home_team": home_team,
                    "away_team": away_team,
                    "league_name": league_name,
                    "match_date": match_date,
                }
            else:
                return {}
        except Exception as e:
            logger.error(f"Erro ao buscar informações do evento: {e}")
            return {}
        finally:
            self.disconnect()

    def get_team_stats(self, team_name: str, stat_type: str, limit: int = 10) -> List[float]:
        """Busca estatísticas históricas reais de uma equipe - SEM FALLBACK"""
        try:
            # ✅ CORRIGIDO: usar lol_history.db ao invés de lol_esports.db
            history_conn = sqlite3.connect("data/lol_history.db")
            cursor = history_conn.cursor()

            # ✅ Buscar pelo nome do time na tabela teams
            cursor.execute("SELECT team_id FROM teams WHERE name = ?", (team_name,))
            team_result = cursor.fetchone()

            if not team_result:
                history_conn.close()
                logger.warning(f"⚠️ Time '{team_name}' não encontrado no banco de dados")
                return []

            team_id = team_result[0]

            # Buscar últimas partidas do time (últimos 60 dias)
            query_matches = """
            SELECT id FROM matches
            WHERE (home_team_id = ? OR away_team_id = ?)
            AND time_status = 3
            AND event_time >= datetime('now', '-60 days')
            ORDER BY event_time DESC
            LIMIT 30
            """

            cursor.execute(query_matches, (team_id, team_id))
            matches = cursor.fetchall()

            if not matches:
                history_conn.close()
                logger.warning(f"⚠️ Nenhuma partida encontrada para '{team_name}' nos últimos 60 dias")
                return []

            # Buscar mapas dessas partidas
            match_ids = [str(match[0]) for match in matches]
            placeholders = ",".join(["?"] * len(match_ids))

            query_maps = f"""
            SELECT id FROM game_maps
            WHERE match_id IN ({placeholders})
            ORDER BY match_id DESC, map_number ASC
            """

            cursor.execute(query_maps, match_ids)
            all_maps = cursor.fetchall()

            if not all_maps:
                history_conn.close()
                logger.warning(f"⚠️ Nenhum mapa encontrado para '{team_name}'")
                return []

            # Buscar estatísticas dos mapas
            map_ids = [str(map_data[0]) for map_data in all_maps]
            placeholders = ",".join(["?"] * len(map_ids))

            query_stats = f"""
            SELECT home_value, away_value
            FROM map_statistics
            WHERE map_id IN ({placeholders})
            AND stat_name = ?
            ORDER BY map_id DESC
            """

            cursor.execute(query_stats, map_ids + [stat_type])
            all_stats = cursor.fetchall()

            # Processar estatísticas
            valid_stats = []
            for stat in all_stats:
                if len(valid_stats) >= limit:
                    break

                home_value, away_value = stat
                try:
                    home_val = float(home_value) if home_value else 0.0
                    away_val = float(away_value) if away_value else 0.0
                    total_value = home_val + away_val

                    # Para inhibitors, ignorar valores zerados
                    if stat_type == "inhibitors" and total_value == 0:
                        continue

                    valid_stats.append(total_value)
                except (ValueError, TypeError):
                    continue

            history_conn.close()

            if len(valid_stats) == 0:
                logger.warning(f"⚠️ Nenhuma estatística válida para '{team_name}' - {stat_type}")
                return []

            logger.info(f"✅ {len(valid_stats)} estatísticas reais encontradas para '{team_name}' - {stat_type}")
            return valid_stats[:limit]

        except Exception as e:
            logger.error(f"❌ Erro ao buscar stats para '{team_name}': {e}")
            return []


    def calculate_roi(
        self,
        historical_data: List[float],
        handicap: float,
        odds: float,
        selection: str,
        debug: bool = True,
        team_name: str = "",
    ) -> Tuple[float, float]:
        """Calcula o ROI e fair_odds baseado em dados históricos"""
        if not historical_data:
            return 0.0, 0.0

        # Conta quantas vezes a aposta teria sido vencedora
        wins = 0
        for value in historical_data:
            if selection.startswith("Over") or "Over" in selection:
                if value > handicap:
                    wins += 1
            elif selection.startswith("Under") or "Under" in selection:
                if value < handicap:
                    wins += 1

        total_bets = len(historical_data)
        if total_bets == 0:
            return 0.0, 0.0

        # Calcula probabilidade
        probability = wins / total_bets

        # Debug
        if debug:
            logger.debug(
                f"      📊 {team_name}: {wins}/{total_bets} wins ({probability:.1%}) - {historical_data}"
            )

        # Calcula ROI
        profit = (wins * odds) - total_bets
        roi = (profit / total_bets) * 100

        # Calcula Fair Odds
        fair_odds = 1 / probability if probability > 0 else float("inf")

        return roi, fair_odds

    def calculate_team_roi(
        self,
        team_name: str,
        selection: str,
        handicap: float,
        odds: float,
        debug: bool = False,
    ) -> Tuple[float, float]:
        """Calcula ROI e fair_odds para um time específico"""
        stat_type = self._get_stat_type(selection)
        if not stat_type:
            return 0.0, 0.0

        team_stats = self.get_team_stats(team_name, stat_type)
        return self.calculate_roi(
            team_stats, handicap, odds, selection, debug, team_name
        )

    def calculate_average_roi(
        self, team1: str, team2: str, selection: str, handicap: float, odds: float
    ) -> Tuple[float, float, float, float]:
        """Calcula ROI e fair_odds para ambos os times"""
        # Calcula para cada time
        roi_team1, fair_odds_team1 = self.calculate_team_roi(
            team1, selection, handicap, odds, debug=True
        )
        roi_team2, fair_odds_team2 = self.calculate_team_roi(
            team2, selection, handicap, odds, debug=True
        )

        # Médias
        roi_average = (roi_team1 + roi_team2) / 2

        # Para fair_odds, calcula probabilidade média
        stat_type = self._get_stat_type(selection)
        if stat_type:
            team1_stats = self.get_team_stats(team1, stat_type)
            team2_stats = self.get_team_stats(team2, stat_type)

            prob1 = self._calculate_probability(team1_stats, handicap, selection)
            prob2 = self._calculate_probability(team2_stats, handicap, selection)
            combined_prob = (prob1 + prob2) / 2

            if combined_prob > 0:
                fair_odds_average = 1 / combined_prob
            else:
                fair_odds_average = 999.99
        else:
            fair_odds_average = 999.99

        return roi_team1, roi_team2, roi_average, fair_odds_average

    def _calculate_probability(
        self, historical_data: List[float], handicap: float, selection: str
    ) -> float:
        """Calcula probabilidade baseada nos dados históricos"""
        if not historical_data:
            return 0.0

        wins = 0
        for value in historical_data:
            if selection.startswith("Over") or "Over" in selection:
                if value > handicap:
                    wins += 1
            elif selection.startswith("Under") or "Under" in selection:
                if value < handicap:
                    wins += 1

        return wins / len(historical_data)

    def get_betting_lines(
        self, event_id: str, market_filter: Optional[str] = None
    ) -> List[Dict]:
        """Retorna todas as linhas de aposta disponíveis para um ou mais mercados"""
        if not self.connect():
            return []

        # Define os mercados e seus respectivos odds_type
        market_mapping = {
            "Map 1 - Totals": "map_1",
            "Map 2 - Totals": "map_2",
        }

        if market_filter is None:
            markets = ["Map 1 - Totals"]
        elif isinstance(market_filter, str):
            markets = [market_filter]
        else:
            markets = market_filter

        all_lines = []

        for market in markets:
            odds_type = market_mapping.get(market, "map_1")
            odds_df = self.get_market_odds(event_id, market, odds_type)

            for _, row in odds_df.iterrows():
                # Converter handicap de string para float
                try:
                    handicap_value = float(row["handicap"])
                except (ValueError, TypeError):
                    continue
                    
                all_lines.append(
                    {
                        "selection": row["selection_name"],
                        "handicap": handicap_value,
                        "odds": row["odds_value"],
                        "updated_at": row["updated_at"],
                        "market_name": row.get("market_name", market),
                    }
                )

        self.disconnect()
        return all_lines

    def _get_stat_type(self, selection_name: str) -> str:
        """Extrai o tipo de estatística do nome da seleção"""
        selection_lower = selection_name.lower()
        
        if "dragon" in selection_lower:
            return "dragons"
        elif "baron" in selection_lower:
            return "barons"
        elif "kill" in selection_lower:
            return "kills"
        elif "tower" in selection_lower:
            return "towers"
        elif "inhibitor" in selection_lower:
            return "inhibitors"
        else:
            return ""