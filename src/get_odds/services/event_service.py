import logging
from datetime import datetime, timedelta
from typing import Dict, List

from src.get_odds.database import OddsDatabase
from src.shared.core.bet365_client import Bet365Client
from src.shared.services.rate_limiter import RateLimiter
from src.shared.utils.validators import is_lol_event

logger = logging.getLogger("lol_odds")


class EventService:
    def __init__(
        self, db: OddsDatabase, client: Bet365Client, rate_limiter: RateLimiter
    ):
        self.db = db
        self.client = client
        self.rate_limiter = rate_limiter
        self.lol_sport_id = 151

    async def fetch_upcoming_events(self, days_ahead: int = 10) -> List[Dict]:
        events = []
        logger.info(f"🔍 Buscando eventos para os próximos {days_ahead} dias")

        for i in range(days_ahead + 1):
            target_date = datetime.now() + timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")

            try:
                logger.info(
                    f"📅 Buscando eventos para {target_date.strftime('%Y-%m-%d')}"
                )
                await self.rate_limiter.acquire()

                daily_events = await self.client.upcoming(
                    sport_id=self.lol_sport_id, day=day_str
                )

                if daily_events.get("success") == 1 and daily_events.get("results"):
                    lol_events = [e for e in daily_events["results"] if is_lol_event(e)]
                    events.extend(lol_events)

                    if lol_events:
                        logger.info(f"   ✅ {len(lol_events)} jogos de LoL encontrados")
                    else:
                        logger.info("   ℹ️  Nenhum jogo de LoL encontrado")
                else:
                    logger.info(f"   ℹ️  Nenhum evento encontrado para {day_str}")

            except Exception as e:
                logger.error(f"❌ Erro ao buscar eventos para {day_str}: {str(e)}")

        logger.info(f"📊 Total de eventos encontrados: {len(events)}")
        return events

    def save_events(self, events: List[Dict]) -> Dict[str, int]:
        stats = {"new": 0, "existing": 0, "updated": 0}

        with self.db.get_connection() as conn:
            for event in events:
                new_event_id = event.get("id")
                home_info = event.get("home", {})
                away_info = event.get("away", {})

                home_team_id = self._get_or_create_team(
                    conn, home_info.get("id"), home_info.get("name", "Unknown")
                )
                away_team_id = self._get_or_create_team(
                    conn, away_info.get("id"), away_info.get("name", "Unknown")
                )

                league_name = event.get("league", {}).get("name", "Unknown")
                match_date, match_timestamp = self._parse_match_time(event.get("time"))

                # Verificar se evento já existe (mesmo event_id)
                cursor = conn.execute(
                    "SELECT 1 FROM events WHERE event_id = ?", (new_event_id,)
                )
                if cursor.fetchone():
                    stats["existing"] += 1
                    continue

                # Verificar duplicata por times + timestamp próximo (±24 horas)
                if match_timestamp:
                    cursor = conn.execute(
                        """
                        SELECT event_id FROM events 
                        WHERE home_team_id = ? AND away_team_id = ?
                        AND ABS(match_timestamp - ?) < 86400
                        AND status = 'upcoming'
                    """,
                        (home_team_id, away_team_id, match_timestamp),
                    )

                    old_event = cursor.fetchone()

                    if old_event:
                        old_event_id = old_event[0]

                        # Deletar odds antigas
                        conn.execute(
                            "DELETE FROM current_odds WHERE event_id = ?",
                            (old_event_id,),
                        )

                        # Atualizar event_id e dados do evento
                        conn.execute(
                            """
                            UPDATE events 
                            SET event_id = ?, league_name = ?, match_date = ?, match_timestamp = ?, updated_at = datetime('now')
                            WHERE event_id = ?
                        """,
                            (
                                new_event_id,
                                league_name,
                                match_date,
                                match_timestamp,
                                old_event_id,
                            ),
                        )

                        stats["updated"] += 1
                        logger.info(
                            f"🔄 Evento atualizado: {home_info.get('name')} vs {away_info.get('name')} - ID: {old_event_id} → {new_event_id}"
                        )
                        continue

                # Inserir novo evento
                conn.execute(
                    """
                    INSERT INTO events 
                    (event_id, home_team_id, away_team_id, league_name, match_date, match_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        new_event_id,
                        home_team_id,
                        away_team_id,
                        league_name,
                        match_date,
                        match_timestamp,
                    ),
                )

                stats["new"] += 1
                logger.info(
                    f"✅ Novo evento: {home_info.get('name')} vs {away_info.get('name')} - {league_name}"
                )

        logger.info(
            f"📝 Eventos processados - Novos: {stats['new']}, Existentes: {stats['existing']}, Atualizados: {stats['updated']}"
        )
        return stats

    def _get_or_create_team(self, conn, team_id: str, team_name: str) -> int:
        cursor = conn.execute("SELECT id FROM teams WHERE team_id = ?", (team_id,))
        existing = cursor.fetchone()

        if existing:
            return existing[0]

        cursor = conn.execute(
            "INSERT INTO teams (team_id, name) VALUES (?, ?)", (team_id, team_name)
        )
        return cursor.lastrowid

    def _parse_match_time(self, timestamp):
        if not timestamp:
            return None, None

        try:
            ts = int(timestamp)
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%Y-%m-%d %H:%M:%S"), ts
        except:
            return None, None
