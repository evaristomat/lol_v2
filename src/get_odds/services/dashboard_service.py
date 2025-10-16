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
        stats = {"new": 0, "existing": 0}

        with self.db.get_connection() as conn:
            for event in events:
                event_id = event.get("id")

                cursor = conn.execute(
                    "SELECT 1 FROM events WHERE event_id = ?", (event_id,)
                )
                if cursor.fetchone():
                    stats["existing"] += 1
                    continue

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

                conn.execute(
                    """
                    INSERT INTO events 
                    (event_id, home_team_id, away_team_id, league_name, match_date, match_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        event_id,
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
            f"📝 Eventos processados - Novos: {stats['new']}, Existentes: {stats['existing']}"
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


import os
from datetime import datetime, timedelta
from pathlib import Path

from src.get_odds.database import OddsDatabase


class DashboardService:
    def __init__(self, db: OddsDatabase):
        self.db = db

    def generate(self) -> str:
        with self.db.get_connection() as conn:
            lines = ["\n" + "=" * 60, "📊 DASHBOARD - LOL ODDS DATABASE", "=" * 60]

            stats = self._get_general_stats(conn)
            lines.extend(
                [
                    f"\n📈 ESTATÍSTICAS GERAIS:",
                    f"  Total de Eventos: {stats['events']:,}",
                    f"  Total de Times: {stats['teams']:,}",
                    f"  Odds Atuais: {stats['odds']:,}",
                    f"  Player Odds: {stats['player_odds']:,}",
                ]
            )

            if os.path.exists(self.db.db_path):
                size_mb = os.path.getsize(self.db.db_path) / 1024 / 1024
                lines.append(f"  Tamanho do Banco: {size_mb:.2f} MB")

            upcoming = self._get_upcoming_stats(conn)
            lines.extend(
                [
                    f"\n🎮 PRÓXIMOS EVENTOS:",
                    f"  Eventos Futuros: {upcoming['total']}",
                    f"  Próximas 24h: {upcoming['next_24h']}",
                ]
            )

            coverage = self._get_coverage_stats(conn)
            lines.extend(
                [
                    f"\n📊 COBERTURA DE ODDS:",
                    f"  Eventos com Odds: {coverage['with_odds']}/{coverage['total']} ({coverage['percentage']:.1f}%)",
                    f"  Eventos com Player Odds: {coverage['with_player_odds']}",
                ]
            )

            last_update = self._get_last_update(conn)
            if last_update:
                lines.append(f"\n⏰ Última Atualização de Odds: {last_update}")

            lines.append("=" * 60 + "\n")

        return "\n".join(lines)

    def save_to_file(self, content: str, report_dir: str = "reports") -> Path:
        report_dir = Path(report_dir)
        report_dir.mkdir(exist_ok=True)
        report_file = report_dir / f"dashboard_{datetime.now():%Y%m%d_%H%M%S}.txt"
        report_file.write_text(content, encoding="utf-8")
        return report_file

    def _get_general_stats(self, conn):
        return {
            "events": conn.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "odds": conn.execute("SELECT COUNT(*) FROM current_odds").fetchone()[0],
            "player_odds": conn.execute(
                "SELECT COUNT(*) FROM current_odds WHERE odds_type = 'player'"
            ).fetchone()[0],
            "teams": conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0],
        }

    def _get_upcoming_stats(self, conn):
        total = conn.execute(
            "SELECT COUNT(*) FROM events WHERE status = 'upcoming'"
        ).fetchone()[0]

        next_24h = conn.execute(
            """
            SELECT COUNT(*) FROM events 
            WHERE match_timestamp BETWEEN strftime('%s', 'now') 
            AND strftime('%s', 'now', '+24 hours')
            AND status = 'upcoming'
        """
        ).fetchone()[0]

        return {"total": total, "next_24h": next_24h}

    def _get_coverage_stats(self, conn):
        cursor = conn.execute(
            """
            SELECT 
                COUNT(DISTINCT e.event_id) as total,
                COUNT(DISTINCT co.event_id) as with_odds
            FROM events e
            LEFT JOIN current_odds co ON e.event_id = co.event_id
            WHERE e.status = 'upcoming'
        """
        )
        total, with_odds = cursor.fetchone()
        percentage = (with_odds / total * 100) if total > 0 else 0

        with_player_odds = conn.execute(
            """
            SELECT COUNT(DISTINCT event_id) 
            FROM current_odds 
            WHERE odds_type = 'player'
        """
        ).fetchone()[0]

        return {
            "total": total,
            "with_odds": with_odds,
            "percentage": percentage,
            "with_player_odds": with_player_odds,
        }

    def _get_last_update(self, conn):
        return conn.execute("SELECT MAX(updated_at) FROM current_odds").fetchone()[0]

    def cleanup_old_data(self, days_keep: int = 30) -> dict:
        cutoff_timestamp = int((datetime.now() - timedelta(days=days_keep)).timestamp())

        with self.db.get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM events WHERE match_timestamp < ?", (cutoff_timestamp,)
            )
            deleted_events = cursor.rowcount

            cursor = conn.execute(
                """
                DELETE FROM teams 
                WHERE team_id NOT IN (
                    SELECT home_team_id FROM events
                    UNION
                    SELECT away_team_id FROM events
                )
            """
            )
            deleted_teams = cursor.rowcount

        with self.db.get_connection() as conn:
            conn.execute("VACUUM")

        return {"deleted_events": deleted_events, "deleted_teams": deleted_teams}
