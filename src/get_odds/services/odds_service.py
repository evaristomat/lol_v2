import asyncio
import json
import logging
import re
import time
from typing import Dict, List, Optional, Tuple

from src.get_odds.database import OddsDatabase
from src.shared.core.bet365_client import Bet365Client
from src.shared.services.rate_limiter import RateLimiter

logger = logging.getLogger("lol_odds")


class OddsService:
    def __init__(
        self, db: OddsDatabase, client: Bet365Client, rate_limiter: RateLimiter
    ):
        self.db = db
        self.client = client
        self.rate_limiter = rate_limiter
        self.semaphore = asyncio.Semaphore(10)
        self.cache = {}

    async def fetch_and_save_odds(
        self, hours_old_threshold: int = 2, batch_size: int = 10
    ) -> int:
        events_to_update = self._get_events_to_update(hours_old_threshold)

        if not events_to_update:
            logger.info("✅ Todos os eventos têm odds atualizadas")
            return 0

        logger.info(
            f"💰 Coletando odds para {len(events_to_update)} eventos em lotes de {batch_size}"
        )
        odds_collected = 0

        for i in range(0, len(events_to_update), batch_size):
            batch = events_to_update[i : i + batch_size]
            logger.info(
                f"   📦 Processando lote {i // batch_size + 1}/{(len(events_to_update) - 1) // batch_size + 1}"
            )

            tasks = [
                self._fetch_odds_for_event(event_id, home, away)
                for event_id, home, away in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"      ❌ Erro em tarefa: {str(result)}")
                elif result:
                    odds_collected += 1

            if i + batch_size < len(events_to_update):
                await asyncio.sleep(0.5)

        logger.info(
            f"📊 Coleta finalizada: {odds_collected}/{len(events_to_update)} eventos com odds"
        )
        return odds_collected

    def _get_events_to_update(self, hours_old: int) -> List[Tuple[str, str, str]]:
        with self.db.get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT DISTINCT e.event_id, ht.name, at.name
                FROM events e
                JOIN teams ht ON e.home_team_id = ht.id
                JOIN teams at ON e.away_team_id = at.id
                LEFT JOIN current_odds co ON e.event_id = co.event_id
                WHERE e.status = 'upcoming'
                AND (co.event_id IS NULL OR datetime(co.updated_at) < datetime('now', ?))
                ORDER BY e.match_timestamp ASC
            """,
                (f"-{hours_old} hours",),
            )
            return cursor.fetchall()

    async def _fetch_odds_for_event(self, event_id: str, home: str, away: str) -> bool:
        async with self.semaphore:
            try:
                cache_key = f"{event_id}_{int(time.time() / 3600)}"
                if cache_key in self.cache:
                    logger.debug(f"      ♻️  Usando cache para {home} vs {away}")
                    return True

                await self.rate_limiter.acquire()

                logger.debug(f"      📡 Buscando odds para {home} vs {away}")
                odds_data = await self.client.prematch(FI=event_id)

                if (
                    odds_data
                    and odds_data.get("success") == 1
                    and odds_data.get("results")
                ):
                    result = odds_data["results"][0]
                    self._save_odds_data(event_id, result)

                    self.cache[cache_key] = True
                    if len(self.cache) > 1000:
                        self.cache.pop(next(iter(self.cache)))

                    logger.debug(f"      ✅ Odds salvas para {home} vs {away}")
                    return True
                else:
                    logger.debug(f"      ⚠️ Sem odds disponíveis para {home} vs {away}")
                    return False

            except Exception as e:
                logger.error(
                    f"      ❌ Erro ao coletar odds para {home} vs {away}: {str(e)}"
                )
                return False

    def _save_odds_data(self, event_id: str, odds_data: Dict):
        with self.db.get_connection() as conn:
            conn.execute("DELETE FROM current_odds WHERE event_id = ?", (event_id,))

            # Salvar FI e bet365_key no evento
            FI = odds_data.get("FI")
            bet365_key = None
            if "main" in odds_data and "key" in odds_data["main"]:
                bet365_key = odds_data["main"]["key"]

            conn.execute(
                """
                UPDATE events 
                SET FI = ?, bet365_key = ? 
                WHERE event_id = ?
            """,
                (FI, bet365_key, event_id),
            )

            total = 0

            # Processar main (match_lines apenas)
            if "main" in odds_data and "sp" in odds_data["main"]:
                total += self._process_main_section(
                    conn, event_id, odds_data["main"]["sp"]
                )

            # Processar map_1 e map_2
            for map_section in ["map_1", "map_2"]:
                if map_section in odds_data and "sp" in odds_data[map_section]:
                    map_num = self._extract_map_number(map_section)
                    total += self._process_map_section(
                        conn,
                        event_id,
                        map_section,
                        odds_data[map_section]["sp"],
                        map_num,
                    )

            # Processar player odds
            if "player" in odds_data and "sp" in odds_data["player"]:
                player_odds = self._process_player_odds(
                    conn, event_id, odds_data["player"]["sp"]
                )
                total += player_odds
                if player_odds > 0:
                    logger.debug(f"      🎮 {player_odds} player odds processadas")

            logger.debug(f"      📊 {total} odds processadas")

    def _process_main_section(self, conn, event_id: str, markets: Dict) -> int:
        """Processa apenas match_lines do main"""
        allowed_markets = {"match_lines"}
        count = 0

        for market_key, market_data in markets.items():
            if market_key not in allowed_markets:
                continue

            if isinstance(market_data, dict) and "odds" in market_data:
                market_type = market_data.get("name", market_key)
                for odd in market_data["odds"]:
                    if isinstance(odd, dict):
                        count += self._save_single_odd(
                            conn, event_id, "main", market_type, odd
                        )

        return count

    def _process_map_section(
        self, conn, event_id: str, section: str, markets: Dict, map_number: int
    ) -> int:
        """Processa apenas mercados permitidos por mapa"""
        allowed_markets = {
            f"{section}_handicaps",
            f"{section}_tower_handicap",
            f"{section}_dragon_handicap",
            f"{section}_totals",
            f"{section}_map_duration_2_way",
        }

        count = 0
        for market_key, market_data in markets.items():
            if market_key not in allowed_markets:
                continue

            if isinstance(market_data, dict) and "odds" in market_data:
                market_type = market_data.get("name", market_key)
                for odd in market_data["odds"]:
                    if isinstance(odd, dict):
                        count += self._save_single_odd(
                            conn, event_id, section, market_type, odd, map_number
                        )

        return count

    def _extract_map_number(self, section: str) -> Optional[int]:
        match = re.search(r"map_(\d+)", section.lower())
        return int(match.group(1)) if match else None

    def _process_player_odds(self, conn, event_id: str, markets: Dict) -> int:
        """Processa apenas player odds permitidos"""
        allowed_markets = {
            "map_1_player_total_kills",
            "map_1_player_total_deaths",
            "map_1_player_total_assists",
            "map_2_player_total_kills",
            "map_2_player_total_deaths",
            "map_2_player_total_assists",
        }

        count = 0
        for market_key, market_data in markets.items():
            if market_key not in allowed_markets:
                continue

            if not isinstance(market_data, dict) or "odds" not in market_data:
                continue

            market_type = market_data.get("name", market_key)
            map_number = self._extract_map_number(market_key)

            for odd in market_data["odds"]:
                if isinstance(odd, dict):
                    count += self._save_single_odd(
                        conn, event_id, "player", market_type, odd, map_number
                    )

        return count

    def _process_odds_section(
        self,
        conn,
        event_id: str,
        section: str,
        markets: Dict,
        map_number: Optional[int] = None,
    ) -> int:
        count = 0
        for market_key, market_data in markets.items():
            if isinstance(market_data, list):
                market_type = f"{section}_{market_key}"
                for odd in market_data:
                    if isinstance(odd, dict):
                        count += self._save_single_odd(
                            conn, event_id, section, market_type, odd, map_number
                        )
                continue

            if not isinstance(market_data, dict) or "odds" not in market_data:
                continue

            market_type = market_data.get("name", market_key)
            for odd in market_data["odds"]:
                if isinstance(odd, dict):
                    count += self._save_single_odd(
                        conn, event_id, section, market_type, odd, map_number
                    )

        return count

    def _save_single_odd(
        self,
        conn,
        event_id: str,
        section: str,
        market_type: str,
        odd: Dict,
        map_number: Optional[int] = None,
    ) -> int:
        try:
            header = odd.get("header", "")
            name = odd.get("name", "")
            selection = f"{header} {name}".strip() if header else name
            odds = float(odd.get("odds", 0))
            line = odd.get("handicap", "")

            if odds == 0:
                return 0

            conn.execute(
                """
                INSERT INTO current_odds 
                (event_id, odds_type, market_type, selection, odds, line, map_number, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    event_id,
                    section,
                    market_type,
                    selection,
                    odds,
                    line,
                    map_number,
                    json.dumps(odd),
                ),
            )

            return 1
        except Exception as e:
            logger.error(f"❌ Erro ao salvar odd: {str(e)}")
            return 0
