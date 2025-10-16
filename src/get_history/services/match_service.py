import logging
from datetime import datetime, timedelta
from typing import Dict, List

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

    async def fetch_recent_matches(self, days_back: int = 2) -> List[Dict]:
        """Busca partidas dos últimos N dias"""
        matches = []
        logger.info(f"🔍 Buscando partidas dos últimos {days_back} dias")

        for i in range(days_back):
            target_date = datetime.now() - timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")

            try:
                logger.info(
                    f"📅 Buscando partidas de {target_date.strftime('%Y-%m-%d')}"
                )
                await self.rate_limiter.acquire()

                daily_events = await self.client.upcoming(
                    sport_id=self.lol_sport_id, day=day_str
                )

                if daily_events.get("success") == 1 and daily_events.get("results"):
                    lol_events = [e for e in daily_events["results"] if is_lol_event(e)]
                    matches.extend(lol_events)

                    if lol_events:
                        logger.info(
                            f"   ✅ {len(lol_events)} partidas de LoL encontradas"
                        )
                    else:
                        logger.info("   ℹ️  Nenhuma partida de LoL encontrada")
                else:
                    logger.info(f"   ℹ️  Nenhum evento para {day_str}")

            except Exception as e:
                logger.error(f"❌ Erro ao buscar partidas para {day_str}: {str(e)}")

        logger.info(f"📊 Total de partidas encontradas: {len(matches)}")
        return matches

    async def process_matches(self, matches: List[Dict]) -> Dict[str, int]:
        """Processa lista de partidas, buscando resultados e salvando"""
        stats = {"new": 0, "updated": 0, "skipped": 0, "errors": 0}

        for event in matches:
            bet365_id = event.get("id")
            home_team = event.get("home", {}).get("name", "Unknown")
            away_team = event.get("away", {}).get("name", "Unknown")

            logger.info(f"⚔️  Processando: {home_team} vs {away_team} - ID: {bet365_id}")

            try:
                # Buscar resultado da partida
                await self.rate_limiter.acquire()
                result_data = await self.client.result(bet365_id)

                if not result_data or result_data.get("success") != 1:
                    logger.warning(f"   ⚠️  Sem resultado para {bet365_id}")
                    stats["skipped"] += 1
                    continue

                result = result_data.get("results", [{}])[0]

                # Salvar ou atualizar
                status = self.save_or_update_match(event, result)
                stats[status] += 1

                if status == "new":
                    logger.info(f"   ✅ Nova partida salva")
                elif status == "updated":
                    logger.info(f"   🔄 Partida atualizada")
                else:
                    logger.info(f"   ⏭️  Partida pulada")

            except Exception as e:
                logger.error(f"   ❌ Erro ao processar {bet365_id}: {str(e)}")
                stats["errors"] += 1

        logger.info(
            f"📝 Resumo - Novas: {stats['new']}, Atualizadas: {stats['updated']}, Puladas: {stats['skipped']}, Erros: {stats['errors']}"
        )
        return stats

    def save_or_update_match(self, event: Dict, result: Dict) -> str:
        """Salva ou atualiza uma partida. Retorna: 'new', 'updated', 'skipped'"""
        bet365_id = event.get("id")

        with self.db.get_connection() as conn:
            cursor = conn.execute(
                "SELECT id, time_status, retrieved_at FROM matches WHERE bet365_id = ?",
                (bet365_id,),
            )
            existing = cursor.fetchone()

            if existing:
                match_id, old_status, retrieved_at = existing

                # Se finalizado (status 3) e atualizado recentemente, skip
                if old_status == 3 and self._is_recently_updated(retrieved_at, hours=1):
                    return "skipped"

                # Atualizar
                self._update_match(conn, bet365_id, match_id, event, result)
                return "updated"
            else:
                # Nova partida
                self._insert_match(conn, event, result)
                return "new"

    def _insert_match(self, conn, event: Dict, result: Dict):
        """Insere nova partida"""
        bet365_id = event.get("id")

        # Salvar liga
        league_id = event.get("league", {}).get("id")
        league_name = event.get("league", {}).get("name")
        if league_id and league_name:
            conn.execute(
                "INSERT OR IGNORE INTO leagues (league_id, name) VALUES (?, ?)",
                (league_id, league_name),
            )

        # Salvar times
        home_team = event.get("home", {})
        away_team = event.get("away", {})

        for team in [home_team, away_team]:
            if team.get("id"):
                conn.execute(
                    "INSERT OR IGNORE INTO teams (team_id, name, image_id, country_code) VALUES (?, ?, ?, ?)",
                    (
                        team.get("id"),
                        team.get("name"),
                        team.get("image_id"),
                        team.get("cc"),
                    ),
                )

        # Salvar partida
        event_time = self._parse_timestamp(event.get("time"))

        cursor = conn.execute(
            """
            INSERT INTO matches 
            (bet365_id, sport_id, league_id, home_team_id, away_team_id, event_time, time_status, final_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                bet365_id,
                event.get("sport_id"),
                league_id,
                home_team.get("id"),
                away_team.get("id"),
                event_time,
                event.get("time_status"),
                result.get("ss"),
            ),
        )

        match_id = cursor.lastrowid

        # Salvar estatísticas de mapas
        self._save_map_stats(conn, match_id, result)

    def _update_match(
        self, conn, bet365_id: str, match_id: int, event: Dict, result: Dict
    ):
        """Atualiza partida existente"""
        conn.execute(
            """
            UPDATE matches 
            SET time_status = ?, final_score = ?, updated_at = datetime('now')
            WHERE bet365_id = ?
        """,
            (event.get("time_status"), result.get("ss"), bet365_id),
        )

        # Se finalizado, salvar/atualizar estatísticas
        if event.get("time_status") == 3:
            self._save_map_stats(conn, match_id, result)

    def _save_map_stats(self, conn, match_id: int, result: Dict):
        """Salva estatísticas dos mapas"""
        period_stats = result.get("period_stats", {})
        if not period_stats:
            return

        for map_number, stats in period_stats.items():
            try:
                # Salvar/atualizar mapa
                conn.execute(
                    "INSERT OR REPLACE INTO game_maps (match_id, map_number) VALUES (?, ?)",
                    (match_id, int(map_number)),
                )

                map_id = conn.execute(
                    "SELECT id FROM game_maps WHERE match_id = ? AND map_number = ?",
                    (match_id, int(map_number)),
                ).fetchone()[0]

                # Salvar estatísticas
                for stat_name, values in stats.items():
                    if isinstance(values, list) and len(values) == 2:
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO map_statistics 
                            (map_id, stat_name, home_value, away_value)
                            VALUES (?, ?, ?, ?)
                        """,
                            (map_id, stat_name, str(values[0]), str(values[1])),
                        )

            except (ValueError, TypeError) as e:
                logger.warning(f"   ⚠️  Erro ao processar mapa {map_number}: {str(e)}")

    def _parse_timestamp(self, timestamp) -> str:
        """Converte timestamp para datetime string"""
        if not timestamp:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        except:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _is_recently_updated(self, retrieved_at: str, hours: int = 1) -> bool:
        """Verifica se foi atualizado recentemente"""
        if not retrieved_at:
            return False

        try:
            last_update = datetime.strptime(retrieved_at, "%Y-%m-%d %H:%M:%S")
            diff = datetime.now() - last_update
            return diff.total_seconds() < (hours * 3600)
        except:
            return False
