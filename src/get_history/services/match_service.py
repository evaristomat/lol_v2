import logging
from datetime import datetime, timedelta

from src.get_history.database import HistoryDatabase
from src.shared.core.bet365_client import Bet365Client
from src.shared.services.rate_limiter import RateLimiter
from src.shared.utils.validators import is_lol_event

logger = logging.getLogger("lol_history")


class MatchService:
    def __init__(
        self, db: HistoryDatabase, client: Bet365Client, rate_limiter: RateLimiter
    ):
        self.db = db
        self.client = client
        self.rate_limiter = rate_limiter
        self.lol_sport_id = 151

    async def fetch_and_process_matches(self, days_back: int) -> dict:
        # 1. Buscar partidas da API
        api_matches = await self.fetch_recent_matches(days_back)
        logger.info(f"📥 {len(api_matches)} partidas da API")

        # 2. Buscar partidas incompletas no banco
        incomplete_matches = self.db.get_incomplete_matches(days_back)
        logger.info(f"🔄 {len(incomplete_matches)} partidas incompletas no banco")

        # 3. Processar partidas da API
        stats = await self.process_api_matches(api_matches)

        # 4. Tentar atualizar partidas incompletas
        incomplete_stats = await self.process_incomplete_matches(incomplete_matches)

        # 5. Consolidar estatísticas
        stats["new"] += incomplete_stats["new"]
        stats["updated"] += incomplete_stats["updated"]
        stats["skipped"] += incomplete_stats["skipped"]
        stats["errors"] += incomplete_stats["errors"]

        return stats

    async def fetch_recent_matches(self, days_back: int) -> list[dict]:
        matches = []
        logger.info(f"🔍 Buscando últimos {days_back} dias")

        for i in range(days_back):
            target_date = datetime.now() - timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")

            try:
                await self.rate_limiter.acquire()
                daily_events = await self.client.upcoming(
                    sport_id=self.lol_sport_id, day=day_str
                )

                if daily_events.get("success") == 1 and daily_events.get("results"):
                    lol_events = [e for e in daily_events["results"] if is_lol_event(e)]
                    matches.extend(lol_events)

                    if lol_events:
                        logger.info(
                            f"✅ {target_date.strftime('%Y-%m-%d')}: {len(lol_events)} partidas"
                        )

            except Exception as e:
                logger.error(f"❌ Erro {day_str}: {e}")

        return matches

    async def process_api_matches(self, matches: list[dict]) -> dict:
        """Processa partidas vindas da API"""
        stats = {"new": 0, "updated": 0, "skipped": 0, "errors": 0}

        for event in matches:
            bet365_id = event.get("id")

            try:
                existing = self.db.get_match(bet365_id)

                if existing and existing.get("time_status") == 3:
                    stats["skipped"] += 1
                    continue

                if existing and self._is_recently_updated(existing.get("updated_at")):
                    stats["skipped"] += 1
                    continue

                result = await self._fetch_match_result(bet365_id)

                if not result:
                    stats["skipped"] += 1
                    continue

                status = self._save_or_update_match(event, result, existing)
                stats[status] += 1

            except Exception as e:
                logger.error(f"❌ Erro {bet365_id}: {e}")
                stats["errors"] += 1

        return stats

    async def process_incomplete_matches(self, incomplete_matches: list[dict]) -> dict:
        """Tenta atualizar partidas incompletas do banco"""
        stats = {"new": 0, "updated": 0, "skipped": 0, "errors": 0}

        if not incomplete_matches:
            return stats

        logger.info(
            f"🔧 Tentando atualizar {len(incomplete_matches)} partidas incompletas"
        )

        for match in incomplete_matches:
            bet365_id = match["bet365_id"]

            try:
                # Verifica se foi atualizada recentemente
                if self._is_recently_updated(match.get("updated_at")):
                    logger.debug(f"⏭️ {bet365_id} atualizada recentemente")
                    stats["skipped"] += 1
                    continue

                logger.info(f"🔄 Tentando atualizar {bet365_id}")

                # Buscar resultado atualizado
                result = await self._fetch_match_result(bet365_id)

                if not result:
                    logger.warning(f"⚠️ Sem resultado para {bet365_id}")
                    stats["skipped"] += 1
                    continue

                # Atualizar no banco
                match_data = {
                    "time_status": result.get("time_status", match["time_status"]),
                    "final_score": result.get("ss", match.get("final_score")),
                }

                self.db.update_match(bet365_id, match_data)

                # Se finalizou, salvar estatísticas dos mapas
                if match_data["time_status"] == 3 and match_data["final_score"]:
                    self._save_map_stats(match["id"], result)
                    logger.info(f"✅ {bet365_id} finalizada e atualizada")
                else:
                    logger.info(f"🔄 {bet365_id} atualizada (ainda não finalizada)")

                stats["updated"] += 1

            except Exception as e:
                logger.error(f"❌ Erro ao atualizar {bet365_id}: {e}")
                stats["errors"] += 1

        return stats

    async def _fetch_match_result(self, bet365_id: str) -> dict | None:
        await self.rate_limiter.acquire()
        result_data = await self.client.result(bet365_id)

        if not result_data or result_data.get("success") != 1:
            return None

        return result_data.get("results", [{}])[0]

    def _save_or_update_match(
        self, event: dict, result: dict, existing: dict | None
    ) -> str:
        bet365_id = event.get("id")

        if existing:
            match_data = self._extract_match_data(event, result)
            self.db.update_match(bet365_id, match_data)

            if event.get("time_status") == 3:
                self._save_map_stats(existing["id"], result)

            return "updated"
        else:
            self._save_league_and_teams(event)
            match_data = self._extract_match_data(event, result)
            match_id = self.db.insert_match(match_data)
            self._save_map_stats(match_id, result)
            return "new"

    def _save_league_and_teams(self, event: dict):
        league = event.get("league", {})
        if league.get("id") and league.get("name"):
            self.db.insert_league(str(league["id"]), league["name"])

        home_team = event.get("home", {})
        if home_team.get("id"):
            self.db.insert_team(
                {
                    "team_id": str(home_team["id"]),
                    "name": home_team.get("name", "Unknown"),
                    "image_id": home_team.get("image_id"),
                    "country_code": home_team.get("cc"),
                }
            )

        away_team = event.get("away", {})
        if away_team.get("id"):
            self.db.insert_team(
                {
                    "team_id": str(away_team["id"]),
                    "name": away_team.get("name", "Unknown"),
                    "image_id": away_team.get("image_id"),
                    "country_code": away_team.get("cc"),
                }
            )

    def _extract_match_data(self, event: dict, result: dict) -> dict:
        home_team = event.get("home", {})
        away_team = event.get("away", {})
        league = event.get("league", {})

        return {
            "bet365_id": event.get("id"),
            "sport_id": str(event.get("sport_id", self.lol_sport_id)),
            "league_id": str(league.get("id")) if league.get("id") else None,
            "home_team_id": str(home_team.get("id")),
            "away_team_id": str(away_team.get("id")),
            "event_time": self._parse_timestamp(event.get("time")),
            "time_status": event.get("time_status"),
            "final_score": result.get("ss"),
        }

    def _save_map_stats(self, match_id: int, result: dict):
        period_stats = result.get("period_stats", {})

        if not period_stats:
            return

        for map_number, stats in period_stats.items():
            try:
                map_id = self.db.insert_game_map(match_id, int(map_number))

                for stat_name, values in stats.items():
                    if isinstance(values, list) and len(values) == 2:
                        self.db.insert_map_statistic(
                            {
                                "map_id": map_id,
                                "stat_name": stat_name,
                                "home_value": str(values[0]),
                                "away_value": str(values[1]),
                            }
                        )

            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️ Erro mapa {map_number}: {e}")

    def _parse_timestamp(self, timestamp) -> str:
        if not timestamp:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        except:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _is_recently_updated(self, updated_at: str, hours: int = 1) -> bool:
        if not updated_at:
            return False

        try:
            last_update = datetime.fromisoformat(updated_at)
            diff = datetime.now() - last_update
            return diff.total_seconds() < (hours * 3600)
        except:
            return False
