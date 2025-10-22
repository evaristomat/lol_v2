"""
Repositórios para acesso a dados
"""

import time
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd

from .cache import cache_manager
from .config import BetStatus, Config, setup_logger
from .database import connection_pool
from .models import BetResult, GameStats, PendingBetAnalysis

logger = setup_logger()


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
    def get_statistics(days_back: int = 7) -> dict:
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
                match_id = matches.iloc[0]["id"]

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
