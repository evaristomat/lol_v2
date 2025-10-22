# main_improved.py - Sistema completo com mapeamento de times e tolerância de data
import logging
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import numpy as np
import pandas as pd
from colorama import Fore, Style, init

# ==================== CONFIGURAÇÃO ====================

init(autoreset=True)

# Mapeamento de nomes de times para LCKC e outras ligas
TEAM_NAME_MAPPINGS = {
    "BNK FearX": "BNK FEARX Youth",
    "BNK FearX.Y": "BNK FEARX Youth",
    "DN Freecs.Ch": "DN Freecs Challengers",
    "DRX.Ch": "DRX Challengers",
    "Dplus KIA.Ch": "Dplus KIA Challengers",
    "Gen.G.GA": "Gen.G Global Academy",
    "Hanwha Life Esports.Ch": "Hanwha Life Esports Challengers",
    "KT Rolster.Ch": "KT Rolster Challengers",
    "Nongshim.EA": "Nongshim Esports Academy",
    "T1.EA": "T1 Esports Academy",
}


class Config:
    """Configuração centralizada do sistema"""

    # Paths
    DB_BETS = "data/lol_bets.db"
    DB_HISTORY = "data/lol_history.db"
    CSV_MATCHES = "data/database/database.csv"

    # Performance
    BATCH_SIZE = 100
    MAX_WORKERS = 4
    CACHE_SIZE = 128
    CONNECTION_TIMEOUT = 30

    # Business rules
    PARTICIPANT_ID_RANGE = (1, 10)  # Para players
    TEAM_PARTICIPANT_IDS = [100, 200]  # Para times

    # Melhorias de matching
    DATE_TOLERANCE_DAYS = 1  # Tolerância de +/- 1 dia para datas
    TEAM_NAME_MAPPINGS = TEAM_NAME_MAPPINGS


# ==================== ENUMS E TIPOS ====================


class BetStatus(Enum):
    """Estados possíveis de uma aposta"""

    PENDING = "pending"
    TO_VERIFY = "to_verify"
    WON = "won"
    LOST = "lost"
    VOID = "void"
    NOT_FOUND = "not_found"


class MarketType(Enum):
    """Tipos de mercado"""

    TOTAL_KILLS = "Total Kills"
    TOTAL_BARONS = "Total Barons"
    TOTAL_DRAGONS = "Total Dragons"
    TOTAL_INHIBITORS = "Total Inhibitors"
    TOTAL_TOWERS = "Total Towers"
    GAME_DURATION = "Game Duration"
    PLAYER_KILLS = "Player Kills"
    PLAYER_ASSISTS = "Player Assists"
    PLAYER_DEATHS = "Player Deaths"


class SelectionType(Enum):
    """Tipos de seleção"""

    OVER = "Over"
    UNDER = "Under"


@dataclass
class BetResult:
    """Resultado de uma aposta processada"""

    bet_id: int
    actual_value: float
    won: bool
    bet_status: BetStatus
    processing_time: float = 0.0


@dataclass
class GameStats:
    """Estatísticas de um jogo"""

    total_kills: Optional[int] = None
    total_barons: Optional[int] = None
    total_dragons: Optional[int] = None
    total_towers: Optional[int] = None
    total_inhibitors: Optional[int] = None
    game_duration: Optional[int] = None


@dataclass
class TeamInfo:
    """Informação de times"""

    home_name: str
    away_name: str
    match_date: datetime


@dataclass
class PendingBetAnalysis:
    """Análise de aposta pendente"""

    bet_id: int
    event_id: str
    market_type: str
    selection: str
    created_at: datetime
    match_date: Optional[datetime]
    days_since_match: Optional[int]
    status: str  # 'future', 'recent', 'old'


# ==================== LOGGING SIMPLIFICADO ====================


class SimpleColoredFormatter(logging.Formatter):
    """Formatter simplificado e robusto"""

    COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        # Formatar a mensagem primeiro
        message = super().format(record)

        # Aplicar cor baseada no nível
        color = self.COLORS.get(record.levelno, "")
        return f"{color}{message}{Style.RESET_ALL}"


def setup_logger():
    """Configura logger de forma robusta"""
    logger = logging.getLogger(__name__)

    # Evitar duplicação de handlers
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()

    # Usar formatter simples
    formatter = SimpleColoredFormatter(
        "%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


logger = setup_logger()

# ==================== GERENCIAMENTO DE CONEXÕES ====================


class ConnectionPool:
    """Pool de conexões para reutilização"""

    def __init__(self):
        self._connections: Dict[str, sqlite3.Connection] = {}
        self._lock = threading.RLock()

    @contextmanager
    def get_connection(self, db_path: str):
        """Context manager para obter conexão"""
        with self._lock:
            if db_path not in self._connections:
                try:
                    conn = sqlite3.connect(
                        db_path,
                        timeout=Config.CONNECTION_TIMEOUT,
                        check_same_thread=False,
                    )
                    # Otimizações SQLite
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA journal_mode=WAL")
                    cursor.execute("PRAGMA synchronous=NORMAL")
                    cursor.execute("PRAGMA cache_size=10000")
                    cursor.execute("PRAGMA temp_store=MEMORY")
                    cursor.close()

                    self._connections[db_path] = conn
                except Exception as e:
                    logger.error("Erro ao conectar com %s: %s", db_path, str(e))
                    raise

            yield self._connections[db_path]

    def close_all(self):
        """Fecha todas as conexões"""
        with self._lock:
            for db_path, conn in self._connections.items():
                try:
                    conn.close()
                except Exception as e:
                    logger.error("Erro ao fechar conexão %s: %s", db_path, str(e))
            self._connections.clear()


# Instância global do pool
connection_pool = ConnectionPool()

# ==================== CACHE MANAGER ====================


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

                # Detectar tabela principal (prioridade para nomes comuns)
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

                # Detectar colunas da tabela principal
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

                    # Aplicar mapeamento de nomes se a coluna teamname existir
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

# ==================== REPOSITÓRIOS ====================


class BetsRepository:
    """Repositório para operações com apostas"""

    @staticmethod
    def get_pending_bets() -> pd.DataFrame:
        """Busca todas as apostas pendentes"""
        try:
            with connection_pool.get_connection(Config.DB_BETS) as conn:
                query = """
                    SELECT b.*, e.match_date 
                    FROM bets b
                    LEFT JOIN events e ON b.event_id = e.event_id
                    WHERE b.bet_status IN (?, ?)
                    ORDER BY b.created_at DESC
                """
                bets = pd.read_sql_query(
                    query,
                    conn,
                    params=[BetStatus.PENDING.value, BetStatus.TO_VERIFY.value],
                )
                logger.info("%d apostas pendentes encontradas", len(bets))
                return bets
        except Exception as e:
            logger.error("Erro ao buscar apostas pendentes: %s", str(e))
            return pd.DataFrame()

    @staticmethod
    def batch_update_results(results: List[BetResult]):
        """Atualiza múltiplas apostas de uma vez"""
        if not results:
            return

        logger.info("Atualizando %d apostas em batch...", len(results))
        start_time = time.time()

        try:
            with connection_pool.get_connection(Config.DB_BETS) as conn:
                cursor = conn.cursor()

                # Preparar dados para batch update
                update_data = []
                for result in results:
                    odds = 2.0  # TODO: Buscar odds reais da aposta
                    actual_win = (odds - 1) if result.won else -1

                    update_data.append(
                        (
                            result.actual_value,
                            actual_win,
                            result.bet_status.value,
                            1,  # result_verified
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            result.bet_id,
                        )
                    )

                # Executar batch update
                cursor.executemany(
                    """
                    UPDATE bets 
                    SET actual_value = ?,
                        actual_win = ?,
                        bet_status = ?,
                        result_verified = ?,
                        updated_at = ?
                    WHERE id = ?
                """,
                    update_data,
                )

                conn.commit()

                elapsed = time.time() - start_time
                logger.info("Batch update concluído em %.2fs", elapsed)

        except Exception as e:
            logger.error("Erro no batch update: %s", str(e))
            raise

    @staticmethod
    def analyze_pending_bets(bets: pd.DataFrame) -> List[PendingBetAnalysis]:
        """Analisa apostas pendentes por data do jogo"""
        analyses = []
        now = datetime.now()
        today = now.date()

        for _, bet in bets.iterrows():
            bet_id = bet["id"]
            event_id = bet["event_id"]
            market_type = bet["market_type"]
            selection = bet["selection"]
            created_at = pd.to_datetime(bet["created_at"])

            # Obter data do jogo
            match_date = None
            days_since_match = None
            status = "unknown"

            team_info = cache_manager.get_teams(event_id)
            if team_info:
                match_date = team_info.match_date
                match_date_date = match_date.date()

                # Calcular dias desde o jogo
                days_since_match = (today - match_date_date).days

                # Classificar status
                if match_date_date > today:
                    status = "future"
                elif days_since_match <= 1:
                    status = "recent"
                elif days_since_match <= 7:
                    status = "recent_week"
                else:
                    status = "old"
            else:
                # Se não encontrou informações do time, verificar pela data de criação
                days_since_creation = (today - created_at.date()).days
                if days_since_creation >= 7:
                    status = "old_no_info"
                else:
                    status = "recent_no_info"

            analysis = PendingBetAnalysis(
                bet_id=bet_id,
                event_id=event_id,
                market_type=market_type,
                selection=selection,
                created_at=created_at,
                match_date=match_date,
                days_since_match=days_since_match,
                status=status,
            )
            analyses.append(analysis)

        return analyses

    @staticmethod
    def get_statistics(days_back: int = 7) -> Dict[str, int]:
        """Obtém estatísticas das apostas"""
        try:
            with connection_pool.get_connection(Config.DB_BETS) as conn:
                now = datetime.now()
                week_ago = now - timedelta(days=days_back)
                yesterday = now - timedelta(days=1)

                # Apostas antigas pendentes
                old_pending = pd.read_sql_query(
                    """
                    SELECT COUNT(*) as count FROM bets 
                    WHERE bet_status IN (?, ?) 
                    AND created_at < ?
                """,
                    conn,
                    params=[
                        BetStatus.PENDING.value,
                        BetStatus.TO_VERIFY.value,
                        week_ago.strftime("%Y-%m-%d"),
                    ],
                )

                # Apostas recentes pendentes
                recent_pending = pd.read_sql_query(
                    """
                    SELECT COUNT(*) as count FROM bets 
                    WHERE bet_status IN (?, ?) 
                    AND created_at >= ?
                """,
                    conn,
                    params=[
                        BetStatus.PENDING.value,
                        BetStatus.TO_VERIFY.value,
                        yesterday.strftime("%Y-%m-%d"),
                    ],
                )

                return {
                    "old_pending": old_pending.iloc[0]["count"],
                    "recent_pending": recent_pending.iloc[0]["count"],
                }
        except Exception as e:
            logger.error("Erro ao buscar estatísticas: %s", str(e))
            return {"old_pending": 0, "recent_pending": 0}


class HistoryRepository:
    """Repositório para lol_history com estrutura adaptativa"""

    @staticmethod
    def get_game_stats(event_id: str, map_num: int) -> Optional[GameStats]:
        """Busca estatísticas do jogo no histórico com estrutura adaptativa"""
        cache_key = (event_id, map_num)
        if cache_key in cache_manager.history_cache:
            return cache_manager.history_cache[cache_key]

        # Detectar estrutura
        structure = cache_manager.detect_history_structure()
        if not structure:
            return None

        try:
            with connection_pool.get_connection(Config.DB_HISTORY) as conn:
                # Primeiro tentar busca direta por event_id
                main_table = structure["main_table"]
                event_id_col = structure["event_id_col"]

                query_direct = f"SELECT * FROM {main_table} WHERE {event_id_col} = ?"
                matches = pd.read_sql_query(query_direct, conn, params=[event_id])

                # Se não encontrou, tentar busca por times e data
                if len(matches) == 0:
                    logger.debug(
                        "Match não encontrado diretamente, tentando busca por times..."
                    )
                    return HistoryRepository._find_match_by_teams_and_date(
                        event_id, map_num, conn, structure
                    )

                # Processar match encontrado
                match_id = matches.iloc[0]["id"]  # Assumindo que há uma coluna 'id'

                # Buscar estatísticas do mapa
                try:
                    game_map = pd.read_sql_query(
                        "SELECT id FROM game_maps WHERE match_id = ? AND map_number = ?",
                        conn,
                        params=[match_id, map_num],
                    )

                    if len(game_map) == 0:
                        logger.debug(
                            "Mapa %d não encontrado para match_id %s", map_num, match_id
                        )
                        return None

                    map_id = game_map.iloc[0]["id"]

                    # Buscar estatísticas
                    stats = pd.read_sql_query(
                        "SELECT stat_name, home_value, away_value FROM map_statistics WHERE map_id = ?",
                        conn,
                        params=[map_id],
                    )

                    if len(stats) == 0:
                        logger.debug(
                            "Estatísticas não encontradas para map_id %s", map_id
                        )
                        return None

                    # Processar estatísticas
                    game_stats = HistoryRepository._process_stats(stats)
                    cache_manager.history_cache[cache_key] = game_stats
                    logger.debug(
                        "Estatísticas encontradas no histórico para %s mapa %d",
                        event_id,
                        map_num,
                    )
                    return game_stats

                except Exception as e:
                    logger.debug("Estrutura de estatísticas não encontrada: %s", e)
                    return None

        except Exception as e:
            logger.error("Erro ao buscar no histórico: %s", str(e))
            return None

    @staticmethod
    def _find_match_by_teams_and_date(
        event_id: str, map_num: int, conn, structure
    ) -> Optional[GameStats]:
        """Busca match por times e data com tolerância"""
        try:
            # Obter informações dos times
            team_info = cache_manager.get_teams(event_id)
            if not team_info:
                return None

            # Aplicar mapeamento de nomes
            home_mapped = cache_manager.map_team_name(team_info.home_name)
            away_mapped = cache_manager.map_team_name(team_info.away_name)

            main_table = structure["main_table"]
            home_col = structure["home_team_col"]
            away_col = structure["away_team_col"]
            date_col = structure["date_col"]

            if not home_col or not away_col or not date_col:
                logger.debug("Colunas necessárias não detectadas para busca por times")
                return None

            # Calcular range de datas
            match_date = team_info.match_date
            start_date = (
                match_date - timedelta(days=Config.DATE_TOLERANCE_DAYS)
            ).strftime("%Y-%m-%d %H:%M:%S")
            end_date = (
                match_date + timedelta(days=Config.DATE_TOLERANCE_DAYS)
            ).strftime("%Y-%m-%d %H:%M:%S")

            # Buscar matches por times e data
            query = f"""
            SELECT * FROM {main_table} 
            WHERE (
                ({home_col} = ? AND {away_col} = ?)
                OR ({home_col} = ? AND {away_col} = ?)
                OR ({home_col} LIKE ? AND {away_col} LIKE ?)
                OR ({home_col} LIKE ? AND {away_col} LIKE ?)
            )
            AND {date_col} BETWEEN ? AND ?
            LIMIT 1
            """

            params = [
                home_mapped,
                away_mapped,  # ordem normal
                away_mapped,
                home_mapped,  # ordem invertida
                f"%{home_mapped}%",
                f"%{away_mapped}%",  # like ordem normal
                f"%{away_mapped}%",
                f"%{home_mapped}%",  # like ordem invertida
                start_date,
                end_date,
            ]

            matches = pd.read_sql_query(query, conn, params=params)

            if len(matches) == 0:
                logger.debug(
                    "Nenhum match encontrado por times e data para %s vs %s",
                    home_mapped,
                    away_mapped,
                )
                return None

            match_id = matches.iloc[0]["id"]
            logger.info(
                "Match encontrado por busca aproximada: %s vs %s",
                home_mapped,
                away_mapped,
            )

            # Buscar estatísticas para o mapa
            try:
                game_map = pd.read_sql_query(
                    "SELECT id FROM game_maps WHERE match_id = ? AND map_number = ?",
                    conn,
                    params=[match_id, map_num],
                )

                if len(game_map) == 0:
                    logger.debug(
                        "Mapa %d não encontrado para match_id %s", map_num, match_id
                    )
                    return None

                map_id = game_map.iloc[0]["id"]

                stats = pd.read_sql_query(
                    "SELECT stat_name, home_value, away_value FROM map_statistics WHERE map_id = ?",
                    conn,
                    params=[map_id],
                )

                if len(stats) == 0:
                    return None

                game_stats = HistoryRepository._process_stats(stats)
                cache_manager.history_cache[(event_id, map_num)] = game_stats
                return game_stats

            except Exception as e:
                logger.debug("Não foi possível buscar estatísticas: %s", e)
                return None

        except Exception as e:
            logger.error("Erro na busca aproximada por times: %s", str(e))
            return None

    @staticmethod
    def _process_stats(stats: pd.DataFrame) -> GameStats:
        """Processa estatísticas do DataFrame"""
        game_stats = GameStats()

        for _, row in stats.iterrows():
            stat = row["stat_name"]
            home = float(row["home_value"]) if pd.notna(row["home_value"]) else 0
            away = float(row["away_value"]) if pd.notna(row["away_value"]) else 0

            if stat == "kills":
                game_stats.total_kills = int(home + away)
            elif stat == "barons":
                game_stats.total_barons = int(home + away)
            elif stat == "dragons":
                game_stats.total_dragons = int(home + away)
            elif stat == "towers":
                game_stats.total_towers = int(home + away)
            elif stat == "inhibitors":
                game_stats.total_inhibitors = int(home + away)
            elif stat == "game_duration":
                game_stats.game_duration = int(home)

        return game_stats


# ==================== PROCESSADORES DE APOSTAS ====================


class BetProcessor:
    """Processador base para apostas"""

    def __init__(self):
        self.csv_data = cache_manager.load_csv()

    def extract_map_number(self, bet: pd.Series) -> Optional[int]:
        """Extrai número do mapa da aposta"""
        if pd.notna(bet.get("map_number")):
            return int(bet["map_number"])

        market = str(bet.get("market_type", ""))
        for i in range(1, 4):
            if f"Map {i}" in market:
                return i
        return None

    def determine_selection_type(self, selection: str) -> Optional[SelectionType]:
        """Determina tipo de seleção (Over/Under)"""
        if "Over" in selection:
            return SelectionType.OVER
        elif "Under" in selection:
            return SelectionType.UNDER
        return None

    def calculate_result(
        self, actual_value: float, line: float, selection_type: SelectionType
    ) -> bool:
        """Calcula se a aposta foi ganha"""
        if selection_type == SelectionType.OVER:
            return actual_value > line
        elif selection_type == SelectionType.UNDER:
            return actual_value < line
        return False


class TotalsProcessor(BetProcessor):
    """Processador para apostas de totais com busca adaptativa"""

    def process(self, bet: pd.Series) -> Optional[BetResult]:
        """Processa uma aposta de totais"""
        start_time = time.time()
        bet_id = bet["id"]
        event_id = bet["event_id"]
        market_type = bet["market_type"]
        selection = bet["selection"]

        logger.info(
            "[Bet %d] Processando: %s - %s - %s",
            bet_id,
            event_id,
            market_type,
            selection,
        )

        try:
            map_num = self.extract_map_number(bet)
            if map_num is None:
                logger.warning("[Bet %d] Mapa não identificado", bet_id)
                return None

            logger.debug("[Bet %d] Mapa identificado: %d", bet_id, map_num)

            # Tentar buscar no histórico primeiro
            logger.debug("[Bet %d] Buscando no histórico...", bet_id)
            game_stats = HistoryRepository.get_game_stats(event_id, map_num)

            # Se não encontrou, tentar no CSV
            if not game_stats:
                logger.debug(
                    "[Bet %d] Não encontrado no histórico, buscando no CSV...", bet_id
                )
                game_stats = self._get_stats_from_csv(event_id, map_num)
            else:
                logger.debug("[Bet %d] Encontrado no histórico", bet_id)

            if not game_stats:
                logger.warning(
                    "[Bet %d] Estatísticas não encontradas em nenhuma fonte", bet_id
                )
                return None

            logger.debug("[Bet %d] Estatísticas encontradas", bet_id)

            # Determinar valor atual baseado no tipo de mercado
            selection_str = str(selection)
            line = float(bet["line"])
            actual_value = self._get_actual_value(selection_str, game_stats)

            if actual_value is None:
                logger.warning(
                    "[Bet %d] Não foi possível obter valor atual para a seleção", bet_id
                )
                return None

            logger.debug(
                "[Bet %d] Valor atual: %.1f, Line: %.1f", bet_id, actual_value, line
            )

            # Calcular resultado
            selection_type = self.determine_selection_type(selection_str)
            if selection_type is None:
                logger.warning("[Bet %d] Tipo de seleção não identificado", bet_id)
                return None

            won = self.calculate_result(actual_value, line, selection_type)
            status_str = "GANHOU" if won else "PERDEU"
            color = Fore.GREEN if won else Fore.RED

            logger.info(
                "%s[Bet %d] RESULTADO: %s (%.1f vs %.1f)%s",
                color,
                bet_id,
                status_str,
                actual_value,
                line,
                Style.RESET_ALL,
            )

            return BetResult(
                bet_id=bet_id,
                actual_value=actual_value,
                won=won,
                bet_status=BetStatus.WON if won else BetStatus.LOST,
                processing_time=time.time() - start_time,
            )

        except Exception as e:
            logger.error("[Bet %d] Erro ao processar: %s", bet_id, str(e))
            return None

    def _get_stats_from_csv(self, event_id: str, map_num: int) -> Optional[GameStats]:
        """Busca estatísticas no CSV com tolerância e mapeamento"""
        cache_key = (event_id, map_num)
        if cache_key in cache_manager.csv_cache:
            return cache_manager.csv_cache[cache_key]

        team_info = cache_manager.get_teams(event_id)
        if not team_info:
            logger.debug("Team info não encontrado para event_id: %s", event_id)
            return None

        if self.csv_data.empty:
            logger.debug("CSV vazio")
            return None

        try:
            # Aplicar mapeamento de nomes
            home_mapped = cache_manager.map_team_name(team_info.home_name)
            away_mapped = cache_manager.map_team_name(team_info.away_name)

            # Calcular range de datas com tolerância
            match_date = team_info.match_date.date()
            start_date = match_date - timedelta(days=Config.DATE_TOLERANCE_DAYS)
            end_date = match_date + timedelta(days=Config.DATE_TOLERANCE_DAYS)

            logger.debug(
                "Buscando no CSV: %s (%s) vs %s (%s) entre %s e %s mapa %d",
                team_info.home_name,
                home_mapped,
                team_info.away_name,
                away_mapped,
                start_date,
                end_date,
                map_num,
            )

            # Buscar dados com tolerância de data
            date_filtered = self.csv_data[
                (self.csv_data["date"] >= start_date)
                & (self.csv_data["date"] <= end_date)
            ]

            # Filtrar por times (usando nomes mapeados)
            team_filtered = date_filtered[
                (date_filtered["teamname"].isin([home_mapped, away_mapped]))
                & (date_filtered["game"] == map_num)
                & (date_filtered["participantid"].isin(Config.TEAM_PARTICIPANT_IDS))
            ]

            if len(team_filtered) != 2:
                logger.debug(
                    "Dados incompletos no CSV: encontradas %d linhas (esperado: 2). Times: %s vs %s",
                    len(team_filtered),
                    home_mapped,
                    away_mapped,
                )

                # Tentar busca mais flexível
                if len(team_filtered) == 0:
                    # Buscar por qualquer um dos times
                    team_filtered = date_filtered[
                        (date_filtered["teamname"].isin([home_mapped, away_mapped]))
                        & (date_filtered["game"] == map_num)
                    ]

                    if len(team_filtered) > 0:
                        logger.debug(
                            "Encontrados dados parciais: %d linhas", len(team_filtered)
                        )
                    else:
                        # Última tentativa: buscar apenas pela data
                        team_filtered = date_filtered[date_filtered["game"] == map_num]
                        logger.debug(
                            "Buscando apenas por data: %d linhas", len(team_filtered)
                        )

                if len(team_filtered) < 2:
                    return None

            # Calcular totais
            game_stats = GameStats(
                total_kills=int(team_filtered["kills"].sum()),
                total_dragons=int(team_filtered["dragons"].sum()),
                total_barons=int(team_filtered["barons"].sum()),
                total_towers=int(team_filtered["towers"].sum()),
                total_inhibitors=int(team_filtered["inhibitors"].sum()),
                game_duration=int(team_filtered["gamelength"].iloc[0]),
            )

            logger.debug(
                "Estatísticas do CSV: Kills=%d, Dragons=%d, Barons=%d, Towers=%d, Inhibitors=%d, Duration=%d",
                game_stats.total_kills,
                game_stats.total_dragons,
                game_stats.total_barons,
                game_stats.total_towers,
                game_stats.total_inhibitors,
                game_stats.game_duration,
            )

            # Cachear resultado
            cache_manager.csv_cache[cache_key] = game_stats
            return game_stats

        except Exception as e:
            logger.error("Erro ao buscar stats no CSV: %s", str(e))
            return None

    def _get_actual_value(self, selection: str, stats: GameStats) -> Optional[float]:
        """Obtém valor atual baseado na seleção"""
        if "Total Kills" in selection:
            return stats.total_kills
        elif "Total Barons" in selection:
            return stats.total_barons
        elif "Total Dragons" in selection:
            return stats.total_dragons
        elif "Total Inhibitors" in selection:
            return stats.total_inhibitors
        elif "Total Towers" in selection:
            return stats.total_towers
        elif "Game Duration" in selection:
            return stats.game_duration
        return None


class PlayerProcessor(BetProcessor):
    """Processador para apostas de jogadores - Versão melhorada"""

    def process(self, bet: pd.Series) -> Optional[BetResult]:
        """Processa uma aposta de jogador com tolerância de data"""
        start_time = time.time()
        bet_id = bet["id"]
        event_id = bet["event_id"]
        market_type = bet["market_type"]
        selection = bet["selection"]

        logger.info(
            "[Bet %d] Processando PLAYER: %s - %s - %s",
            bet_id,
            event_id,
            market_type,
            selection,
        )

        try:
            team_info = cache_manager.get_teams(event_id)
            if not team_info:
                logger.warning(
                    "[Bet %d] Times não encontrados para event_id: %s", bet_id, event_id
                )
                return None

            if self.csv_data.empty:
                logger.warning("[Bet %d] CSV vazio", bet_id)
                return None

            # Aplicar mapeamento de nomes
            home_mapped = cache_manager.map_team_name(team_info.home_name)
            away_mapped = cache_manager.map_team_name(team_info.away_name)

            # Calcular range de datas com tolerância
            match_date = team_info.match_date.date()
            start_date = match_date - timedelta(days=Config.DATE_TOLERANCE_DAYS)
            end_date = match_date + timedelta(days=Config.DATE_TOLERANCE_DAYS)

            logger.debug(
                "[Bet %d] Times (mapeados): %s (%s) vs %s (%s) entre %s e %s",
                bet_id,
                team_info.home_name,
                home_mapped,
                team_info.away_name,
                away_mapped,
                start_date,
                end_date,
            )

            # Filtrar dados do match com tolerância
            match_data = self.csv_data[
                (self.csv_data["date"] >= start_date)
                & (self.csv_data["date"] <= end_date)
                & (self.csv_data["teamname"].isin([home_mapped, away_mapped]))
            ]

            if len(match_data) == 0:
                logger.warning("[Bet %d] Dados do match não encontrados no CSV", bet_id)
                return None

            logger.debug(
                "[Bet %d] Encontrados %d registros no CSV", bet_id, len(match_data)
            )

            # Filtrar por mapa se especificado
            map_num = self.extract_map_number(bet)
            if map_num:
                match_data = match_data[match_data["game"] == map_num]
                logger.debug(
                    "[Bet %d] Filtrado para mapa %d: %d registros",
                    bet_id,
                    map_num,
                    len(match_data),
                )

            # Extrair nome do jogador
            selection_str = str(selection)
            player_name = selection_str.split()[-1]

            logger.debug("[Bet %d] Buscando jogador: %s", bet_id, player_name)

            # Buscar dados do jogador
            player_data = match_data[
                (match_data["playername"] == player_name)
                & (match_data["participantid"] >= Config.PARTICIPANT_ID_RANGE[0])
                & (match_data["participantid"] <= Config.PARTICIPANT_ID_RANGE[1])
            ]

            if len(player_data) == 0:
                logger.warning("[Bet %d] Player %s não encontrado", bet_id, player_name)
                # Debug: listar jogadores disponíveis
                available_players = match_data["playername"].unique()
                logger.debug(
                    "[Bet %d] Jogadores disponíveis: %s",
                    bet_id,
                    list(available_players),
                )
                return None

            player_stats = player_data.iloc[0]
            line = float(bet["line"])
            market = str(market_type)

            # Determinar valor atual
            actual_value = self._get_player_stat(player_stats, market)
            if actual_value is None:
                logger.warning(
                    "[Bet %d] Não foi possível obter estatística do jogador", bet_id
                )
                return None

            logger.debug(
                "[Bet %d] Estatística do jogador: %.1f, Line: %.1f",
                bet_id,
                actual_value,
                line,
            )

            # Calcular resultado
            selection_type = self.determine_selection_type(selection_str)
            if selection_type is None:
                logger.warning("[Bet %d] Tipo de seleção não identificado", bet_id)
                return None

            won = self.calculate_result(actual_value, line, selection_type)
            status_str = "GANHOU" if won else "PERDEU"
            color = Fore.GREEN if won else Fore.RED

            logger.info(
                "%s[Bet %d] RESULTADO: %s (%.1f vs %.1f)%s",
                color,
                bet_id,
                status_str,
                actual_value,
                line,
                Style.RESET_ALL,
            )

            return BetResult(
                bet_id=bet_id,
                actual_value=actual_value,
                won=won,
                bet_status=BetStatus.WON if won else BetStatus.LOST,
                processing_time=time.time() - start_time,
            )

        except Exception as e:
            logger.error("[Bet %d] Erro ao processar: %s", bet_id, str(e))
            return None

    def _get_player_stat(self, player_stats: pd.Series, market: str) -> Optional[float]:
        """Obtém estatística do jogador baseada no mercado"""
        if "Kills" in market:
            return int(player_stats["kills"])
        elif "Assists" in market:
            return int(player_stats["assists"])
        elif "Deaths" in market:
            return int(player_stats["deaths"])
        return None


# ==================== ORQUESTRADOR PRINCIPAL ====================


class BetVerificationSystem:
    """Sistema principal de verificação de apostas"""

    def __init__(self):
        self.totals_processor = TotalsProcessor()
        self.player_processor = PlayerProcessor()
        self.stats = {BetStatus.WON: 0, BetStatus.LOST: 0, BetStatus.NOT_FOUND: 0}

    def is_player_bet(self, market_type: str) -> bool:
        """Verifica se é aposta de jogador"""
        return "Player" in str(market_type)

    def process_bet(self, bet: pd.Series) -> Optional[BetResult]:
        """Processa uma aposta individual"""
        try:
            if self.is_player_bet(bet["market_type"]):
                return self.player_processor.process(bet)
            else:
                return self.totals_processor.process(bet)
        except Exception as e:
            logger.error("Erro ao processar aposta %d: %s", bet["id"], str(e))
            return None

    def process_batch_parallel(self, bets: pd.DataFrame) -> List[BetResult]:
        """Processa lote de apostas em paralelo"""
        results = []

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            # Submeter todas as tarefas
            future_to_bet = {
                executor.submit(self.process_bet, bet): idx
                for idx, bet in bets.iterrows()
            }

            # Processar resultados conforme completam
            for future in as_completed(future_to_bet):
                idx = future_to_bet[future]
                try:
                    result = future.result(timeout=30)
                    if result:
                        results.append(result)
                        self.stats[result.bet_status] += 1

                        # Log do resultado com cores manuais
                        if result.won:
                            status_str = f"{Fore.GREEN}WON{Style.RESET_ALL}"
                        else:
                            status_str = f"{Fore.RED}LOST{Style.RESET_ALL}"

                        logger.info(
                            "  [%d/%d] Aposta %d: %s (Valor: %.1f, Tempo: %.2fs)",
                            idx + 1,
                            len(bets),
                            result.bet_id,
                            status_str,
                            result.actual_value,
                            result.processing_time,
                        )
                    else:
                        self.stats[BetStatus.NOT_FOUND] += 1
                        logger.info(
                            "  [%d/%d] Aposta %d: %sNÃO ENCONTRADA%s",
                            idx + 1,
                            len(bets),
                            future_to_bet[future],
                            Fore.YELLOW,
                            Style.RESET_ALL,
                        )

                except Exception as e:
                    logger.error("Erro no processamento paralelo: %s", str(e))
                    self.stats[BetStatus.NOT_FOUND] += 1

        return results

    def analyze_pending_bets(self, bets: pd.DataFrame):
        """Analisa e exibe detalhes das apostas pendentes"""
        logger.info(
            "\n%sANÁLISE DETALHADA DAS APOSTAS PENDENTES%s", Fore.CYAN, Style.RESET_ALL
        )

        analyses = BetsRepository.analyze_pending_bets(bets)

        # Contar por categoria
        categories = {
            "future": {"count": 0, "bets": []},
            "recent": {"count": 0, "bets": []},
            "recent_week": {"count": 0, "bets": []},
            "old": {"count": 0, "bets": []},
            "old_no_info": {"count": 0, "bets": []},
            "recent_no_info": {"count": 0, "bets": []},
        }

        for analysis in analyses:
            categories[analysis.status]["count"] += 1
            categories[analysis.status]["bets"].append(analysis)

        # Exibir resumo por categoria
        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}📊 DISTRIBUIÇÃO DAS APOSTAS PENDENTES{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

        total_bets = len(analyses)

        # Jogos futuros (normais)
        future_count = categories["future"]["count"]
        print(
            f"\n{Fore.GREEN}🎯 JOGOS FUTUROS: {future_count} apostas ({future_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Apostas em jogos que ainda vão acontecer)")

        # Jogos recentes (últimas 24h)
        recent_count = categories["recent"]["count"]
        print(
            f"\n{Fore.BLUE}🕐 JOGOS RECENTES (≤1 dia): {recent_count} apostas ({recent_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Jogos que aconteceram nas últimas 24h)")

        # Jogos da última semana (2-7 dias)
        recent_week_count = categories["recent_week"]["count"]
        print(
            f"\n{Fore.YELLOW}📅 JOGOS DA SEMANA (2-7 dias): {recent_week_count} apostas ({recent_week_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Jogos que aconteceram há 2-7 dias)")

        # Jogos antigos (>7 dias) - PROBLEMÁTICOS
        old_count = categories["old"]["count"]
        print(
            f"\n{Fore.RED}🚨 JOGOS ANTIGOS (>7 dias): {old_count} apostas ({old_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Jogos que aconteceram há mais de 7 dias - PRECISAM DE ATENÇÃO!)")

        # Apostas sem informação de times
        old_no_info = categories["old_no_info"]["count"]
        recent_no_info = categories["recent_no_info"]["count"]
        print(
            f"\n{Fore.MAGENTA}❓ SEM INFORMAÇÃO DE TIMES: {old_no_info + recent_no_info} apostas ({(old_no_info + recent_no_info)/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   - Antigas (>7 dias): {old_no_info}")
        print(f"   - Recentes (≤7 dias): {recent_no_info}")

        # Exibir detalhes dos jogos problemáticos
        if categories["old"]["count"] > 0:
            print(
                f"\n{Fore.RED}{'🚨 APOSTAS PROBLEMÁTICAS (JOGOS ANTIGOS) 🚨':^60}{Style.RESET_ALL}"
            )
            for analysis in categories["old"]["bets"][
                :10
            ]:  # Mostrar apenas as 10 primeiras
                print(
                    f"   Bet {analysis.bet_id}: {analysis.event_id} - {analysis.market_type}"
                )
                print(
                    f"        {analysis.selection} | Jogo há {analysis.days_since_match} dias"
                )

            if len(categories["old"]["bets"]) > 10:
                print(f"   ... e mais {len(categories['old']['bets']) - 10} apostas")

        if categories["old_no_info"]["count"] > 0:
            print(
                f"\n{Fore.MAGENTA}{'❓ APOSTAS SEM INFO DE TIMES (ANTIGAS) ❓':^60}{Style.RESET_ALL}"
            )
            for analysis in categories["old_no_info"]["bets"][:5]:
                print(
                    f"   Bet {analysis.bet_id}: {analysis.event_id} - {analysis.market_type}"
                )
                print(
                    f"        {analysis.selection} | Criada há {(datetime.now().date() - analysis.created_at.date()).days} dias"
                )

        return categories, analyses

    def run(self):
        """Executa o sistema de verificação"""
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}")
        print(f"{Fore.CYAN}{Style.BRIGHT}🚀 SISTEMA DE VERIFICAÇÃO DE RESULTADOS v2.0")
        print(
            f"{Fore.CYAN}{Style.BRIGHT}📝 COM MAPEAMENTO DE TIMES E TOLERÂNCIA DE DATA"
        )
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")

        # Exibir informações das melhorias
        print(f"{Fore.GREEN}✅ Melhorias implementadas:{Style.RESET_ALL}")
        print(f"   • Mapeamento de {len(TEAM_NAME_MAPPINGS)} nomes de times")
        print(f"   • Tolerância de +/- {Config.DATE_TOLERANCE_DAYS} dia(s) para datas")
        print(f"   • Busca flexível por nomes similares")
        print()

        start_time = time.time()

        try:
            # Carregar apostas pendentes
            logger.info("Iniciando verificação de resultados...")
            bets = BetsRepository.get_pending_bets()

            if len(bets) == 0:
                logger.info("Nenhuma aposta pendente")
                return

            # Análise detalhada das apostas pendentes
            categories, all_analyses = self.analyze_pending_bets(bets)

            # Criar dicionário para acesso rápido às análises
            analysis_dict = {analysis.bet_id: analysis for analysis in all_analyses}

            # Separar apostas para processamento (apenas as que devem ter resultados)
            bets_to_process = []
            for _, bet in bets.iterrows():
                bet_id = bet["id"]
                analysis = analysis_dict.get(bet_id)
                if analysis and analysis.status in [
                    "recent",
                    "recent_week",
                    "old",
                    "old_no_info",
                ]:
                    bets_to_process.append(bet)

            bets_to_process_df = (
                pd.DataFrame(bets_to_process) if bets_to_process else pd.DataFrame()
            )

            logger.info(
                "\nProcessando %d apostas (jogos que já ocorreram) de %d totais",
                len(bets_to_process_df),
                len(bets),
            )

            if len(bets_to_process_df) == 0:
                logger.info("Nenhuma aposta de jogos passados para processar")
                self._display_summary(0, time.time() - start_time, categories)
                return

            # Pré-carregar cache
            logger.info("Pré-carregando dados em cache...")
            cache_manager.load_csv()

            # Processar em lotes
            logger.info(
                "Processando %d apostas em paralelo...", len(bets_to_process_df)
            )
            all_results = []

            for i in range(0, len(bets_to_process_df), Config.BATCH_SIZE):
                batch = bets_to_process_df.iloc[i : i + Config.BATCH_SIZE]
                logger.info(
                    "Processando lote %d (%d apostas)...",
                    i // Config.BATCH_SIZE + 1,
                    len(batch),
                )

                batch_results = self.process_batch_parallel(batch)
                all_results.extend(batch_results)

                # Atualizar banco em batch
                if batch_results:
                    BetsRepository.batch_update_results(batch_results)
                    logger.info(
                        "Lote %d atualizado: %d apostas processadas",
                        i // Config.BATCH_SIZE + 1,
                        len(batch_results),
                    )
                else:
                    logger.warning(
                        "Lote %d: nenhuma aposta processada", i // Config.BATCH_SIZE + 1
                    )

            # Exibir resumo
            self._display_summary(
                len(bets_to_process_df), time.time() - start_time, categories
            )

        except Exception as e:
            logger.error("Erro fatal no sistema: %s", str(e))
            raise
        finally:
            # Limpeza
            connection_pool.close_all()
            cache_manager.clear()

    def _display_summary(
        self, total_processed: int, elapsed_time: float, categories: Dict = None
    ):
        """Exibe resumo da execução"""
        print(f"{Fore.CYAN}{Style.BRIGHT}\n{'=' * 60}")
        print(f"{Fore.CYAN}{Style.BRIGHT}📊 RESUMO FINAL")
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")

        total_won_lost = sum(self.stats[s] for s in [BetStatus.WON, BetStatus.LOST])

        print(f"{Fore.WHITE}{Style.BRIGHT}\n⏱️  Tempo total: {elapsed_time:.2f}s")
        if elapsed_time > 0 and total_processed > 0:
            print(
                f"{Fore.WHITE}{Style.BRIGHT}📈 Taxa: {total_processed / elapsed_time:.1f} apostas/s"
            )

        if categories:
            total_bets = sum(cat["count"] for cat in categories.values())
            print(
                f"{Fore.WHITE}{Style.BRIGHT}\n📋 Total de apostas pendentes: {total_bets}"
            )
            print(f"{Fore.GREEN}  ✓ Jogos futuros: {categories['future']['count']}")
            print(
                f"{Fore.BLUE}  ✓ Jogos recentes (≤1 dia): {categories['recent']['count']}"
            )
            print(
                f"{Fore.YELLOW}  ✓ Jogos da semana (2-7 dias): {categories['recent_week']['count']}"
            )
            print(
                f"{Fore.RED}  🚨 Jogos antigos (>7 dias): {categories['old']['count']}"
            )
            print(
                f"{Fore.MAGENTA}  ❓ Sem info times: {categories['old_no_info']['count'] + categories['recent_no_info']['count']}"
            )

        print(
            f"{Fore.WHITE}{Style.BRIGHT}\n🔍 Apostas processadas (jogos passados): {total_processed}"
        )
        print(f"{Fore.GREEN}{Style.BRIGHT}  ✓ Apostas atualizadas: {total_won_lost}")
        print(f"{Fore.GREEN}    - Ganhas: {self.stats[BetStatus.WON]}")
        print(f"{Fore.RED}    - Perdidas: {self.stats[BetStatus.LOST]}")
        print(f"{Fore.YELLOW}  ⚠ Sem resultado: {self.stats[BetStatus.NOT_FOUND]}")

        # Taxa de sucesso
        if total_won_lost > 0:
            win_rate = (self.stats[BetStatus.WON] / total_won_lost) * 100
            print(f"{Fore.CYAN}\n🎯 Taxa de acerto: {win_rate:.1f}%")

        # Recomendações
        if categories and categories["old"]["count"] > 0:
            print(
                f"{Fore.RED}\n🚨 RECOMENDAÇÃO: {categories['old']['count']} apostas com jogos antigos precisam de atenção!"
            )
            print(
                f"   Verifique se os dados desses jogos estão disponíveis no CSV/Histórico{Style.RESET_ALL}"
            )

        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")


# ==================== EXECUÇÃO PRINCIPAL ====================


def main():
    """Função principal"""
    try:
        system = BetVerificationSystem()
        system.run()
    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário")
    except Exception as e:
        logger.error("Erro fatal: %s", str(e))
        raise


if __name__ == "__main__":
    main()
