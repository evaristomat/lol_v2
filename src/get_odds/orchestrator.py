import logging
from datetime import datetime
from pathlib import Path

from src.get_odds.database import OddsDatabase
from src.get_odds.services.dashboard_service import DashboardService
from src.get_odds.services.event_service import EventService
from src.get_odds.services.odds_service import OddsService
from src.shared.core.bet365_client import Bet365Client
from src.shared.services.rate_limiter import RateLimiter

logger = logging.getLogger("lol_odds")


class OddsOrchestrator:
    def __init__(self):
        self.db = OddsDatabase()
        self.client = Bet365Client()
        self.rate_limiter = RateLimiter(max_requests=3500, time_window=3600)

        self.event_service = EventService(self.db, self.client, self.rate_limiter)
        self.odds_service = OddsService(self.db, self.client, self.rate_limiter)
        self.dashboard_service = DashboardService(self.db)

        logger.info(f"📀 Database inicializado: {self.db.db_path}")

    async def run(self):
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO ATUALIZAÇÃO DO BANCO DE ODDS LOL")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            await self._fetch_events()
            await self._update_odds()
            self._weekly_cleanup()
            self._show_dashboard()

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"\n✅ ATUALIZAÇÃO COMPLETA EM {elapsed:.1f} SEGUNDOS")

        except Exception as e:
            logger.error(f"❌ Erro durante atualização: {str(e)}", exc_info=True)
            raise
        finally:
            await self.client.close()
            logger.info("🔌 Conexões fechadas")

    async def _fetch_events(self):
        logger.info("\n📅 FASE 1: Buscando eventos...")
        events = await self.event_service.fetch_upcoming_events(days_ahead=10)

        if events:
            stats = self.event_service.save_events(events)
            logger.info(
                f"✅ Fase 1 concluída - {stats['new']} novos eventos adicionados"
            )
        else:
            logger.warning("ℹ️ Nenhum evento encontrado")

    async def _update_odds(self):
        logger.info("\n💰 FASE 2: Atualizando odds...")
        odds_collected = await self.odds_service.fetch_and_save_odds(
            hours_old_threshold=2, batch_size=10
        )
        logger.info(
            f"✅ Fase 2 concluída - {odds_collected} eventos com odds atualizadas"
        )

    def _weekly_cleanup(self):
        if datetime.now().weekday() == 0:
            logger.info("\n🧹 FASE 3: Limpeza semanal...")
            result = self.dashboard_service.cleanup_old_data(days_keep=30)
            logger.info(
                f"🧹 Removidos: {result['deleted_events']} eventos, {result['deleted_teams']} times"
            )

    def _show_dashboard(self):
        dashboard = self.dashboard_service.generate()
        print(dashboard)

        dashboard_file = self.dashboard_service.save_to_file(
            dashboard, report_dir=Path(__file__).parent.parent.parent / "reports"
        )
        logger.info(f"💾 Dashboard salvo em: {dashboard_file}")
