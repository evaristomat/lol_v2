"""
Processadores de apostas
"""

import time
from datetime import timedelta
from typing import Optional

import pandas as pd

from .cache import cache_manager
from .config import Config, SelectionType, setup_logger
from .models import BetResult, BetStatus, GameStats
from .repositories import HistoryRepository

logger = setup_logger()


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

            logger.info(
                "[Bet %d] RESULTADO: %s (%.1f vs %.1f)",
                bet_id,
                status_str,
                actual_value,
                line,
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

            logger.info(
                "[Bet %d] RESULTADO: %s (%.1f vs %.1f)",
                bet_id,
                status_str,
                actual_value,
                line,
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
