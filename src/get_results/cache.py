"""
Gerenciamento de cache do sistema
"""

import sqlite3
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from .config import Config, setup_logger
from .database import connection_pool
from .models import GameStats, TeamInfo

logger = setup_logger()


class CacheManager:
    """Gerenciador de cache centralizado com detecção automática de estrutura"""

    def __init__(self):
        self.csv_data: Optional[pd.DataFrame] = None
        self.teams_cache: Dict[str, TeamInfo] = {}
        self.history_cache: Dict[Tuple[str, int], GameStats] = {}
        self.csv_cache: Dict[Tuple[str, int], GameStats] = {}
        self.team_name_mappings = Config.TEAM_NAME_MAPPINGS
        self._history_structure = None

    def detect_history_structure(self):
        """Detecta automaticamente a estrutura do banco de histórico"""
        if self._history_structure is not None:
            return self._history_structure

        try:
            with connection_pool.get_connection(Config.DB_HISTORY) as conn:
                cursor = conn.cursor()

                # Listar tabelas
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [table[0] for table in cursor.fetchall()]

                # Detectar tabela principal
                main_table_candidates = [
                    "matches",
                    "match",
                    "match_history",
                    "events",
                    "game_history",
                ]
                main_table = next(
                    (t for t in main_table_candidates if t in tables),
                    tables[0] if tables else None,
                )

                if not main_table:
                    logger.warning("Nenhuma tabela encontrada no histórico")
                    return None

                # Detectar colunas
                cursor.execute(f"PRAGMA table_info({main_table})")
                columns = [col[1] for col in cursor.fetchall()]

                # Detectar coluna de ID do evento
                event_id_candidates = [
                    "bet365_id",
                    "event_id",
                    "id",
                    "match_id",
                    "eventId",
                ]
                event_id_col = next(
                    (col for col in event_id_candidates if col in columns),
                    columns[0] if columns else None,
                )

                # Detectar colunas de times
                home_team_candidates = [
                    col
                    for col in columns
                    if "home" in col.lower()
                    and any(x in col.lower() for x in ["team", "name"])
                ]
                away_team_candidates = [
                    col
                    for col in columns
                    if "away" in col.lower()
                    and any(x in col.lower() for x in ["team", "name"])
                ]

                home_team_col = (
                    home_team_candidates[0] if home_team_candidates else None
                )
                away_team_col = (
                    away_team_candidates[0] if away_team_candidates else None
                )

                # Detectar coluna de data
                date_candidates = ["match_date", "date", "game_date", "start_time"]
                date_col = next(
                    (col for col in date_candidates if col in columns), None
                )

                self._history_structure = {
                    "main_table": main_table,
                    "event_id_col": event_id_col,
                    "home_team_col": home_team_col,
                    "away_team_col": away_team_col,
                    "date_col": date_col,
                    "all_columns": columns,
                }

                logger.info(
                    f"Estrutura do histórico detectada: {self._history_structure}"
                )
                return self._history_structure

        except Exception as e:
            logger.error(f"Erro ao detectar estrutura do histórico: {e}")
            return None

    def map_team_name(self, team_name: str) -> str:
        """Aplica mapeamento de nomes de times"""
        return self.team_name_mappings.get(team_name, team_name)

    def load_csv(self) -> pd.DataFrame:
        """Carrega CSV uma única vez com mapeamento automático"""
        if self.csv_data is None:
            logger.info("Carregando CSV em memória...")
            start_time = time.time()

            if not Path(Config.CSV_MATCHES).exists():
                logger.error("CSV não encontrado: %s", Config.CSV_MATCHES)
                self.csv_data = pd.DataFrame()
            else:
                try:
                    self.csv_data = pd.read_csv(Config.CSV_MATCHES, low_memory=False)

                    # Processar datas
                    if "date" in self.csv_data.columns:
                        self.csv_data["date"] = pd.to_datetime(
                            self.csv_data["date"]
                        ).dt.date

                    # Aplicar mapeamento de nomes
                    if "teamname" in self.csv_data.columns:
                        self.csv_data["teamname_original"] = self.csv_data["teamname"]
                        self.csv_data["teamname"] = self.csv_data["teamname"].apply(
                            lambda x: self.map_team_name(x) if pd.notna(x) else x
                        )
                        logger.info(
                            f"Aplicado mapeamento em {len(self.csv_data)} linhas do CSV"
                        )

                    load_time = time.time() - start_time
                    logger.info(
                        "CSV carregado: %d linhas em %.2fs",
                        len(self.csv_data),
                        load_time,
                    )
                except Exception as e:
                    logger.error("Erro ao carregar CSV: %s", str(e))
                    self.csv_data = pd.DataFrame()

        return self.csv_data

    def get_teams(self, event_id: str) -> Optional[TeamInfo]:
        """Obtém informações dos times com cache"""
        if event_id in self.teams_cache:
            return self.teams_cache[event_id]

        try:
            with connection_pool.get_connection(Config.DB_BETS) as conn:
                query = """
                    SELECT e.match_date,
                           t1.name as home_name, 
                           t2.name as away_name
                    FROM events e
                    JOIN teams t1 ON e.home_team_id = t1.id
                    JOIN teams t2 ON e.away_team_id = t2.id
                    WHERE e.event_id = ?
                """
                result = pd.read_sql_query(query, conn, params=[event_id])

                if len(result) > 0:
                    row = result.iloc[0]
                    team_info = TeamInfo(
                        home_name=row["home_name"],
                        away_name=row["away_name"],
                        match_date=pd.to_datetime(row["match_date"]),
                    )
                    self.teams_cache[event_id] = team_info
                    return team_info
                else:
                    logger.debug("Times não encontrados para event_id: %s", event_id)

        except Exception as e:
            logger.error("Erro ao buscar times: %s", str(e))

        return None

    def clear(self):
        """Limpa todos os caches"""
        self.csv_data = None
        self.teams_cache.clear()
        self.history_cache.clear()
        self.csv_cache.clear()
        self._history_structure = None


# Instância global do cache
cache_manager = CacheManager()
