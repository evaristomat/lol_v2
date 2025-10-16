import logging
from datetime import datetime

from src.get_history.database import HistoryDatabase
from src.get_history.services.match_service import MatchService
from src.shared.core.bet365_client import Bet365Client
from src.shared.services.rate_limiter import RateLimiter

logger = logging.getLogger("lol_history")


class HistoryOrchestrator:
    def __init__(self):
        self.db = HistoryDatabase()
        self.client = Bet365Client()
        self.rate_limiter = RateLimiter(max_requests=3500, time_window=3600)

        self.match_service = MatchService(self.db, self.client, self.rate_limiter)

        logger.info(f"📀 Database inicializado: {self.db.db_path}")

    async def run(self, days_back: int = 2):
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO ATUALIZAÇÃO DO HISTÓRICO DE PARTIDAS")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            await self._fetch_and_process_matches(days_back)

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"\n✅ ATUALIZAÇÃO COMPLETA EM {elapsed:.1f} SEGUNDOS")

        except Exception as e:
            logger.error(f"❌ Erro durante atualização: {str(e)}", exc_info=True)
            raise
        finally:
            await self.client.close()
            logger.info("🔌 Conexões fechadas")

    async def _fetch_and_process_matches(self, days_back: int):
        logger.info(f"\n📅 FASE 1: Buscando partidas dos últimos {days_back} dias...")
        matches = await self.match_service.fetch_recent_matches(days_back)

        if not matches:
            logger.warning("ℹ️ Nenhuma partida encontrada")
            return

        logger.info(f"✅ {len(matches)} partidas encontradas")

        logger.info("\n⚔️  FASE 2: Processando partidas e buscando resultados...")
        stats = await self.match_service.process_matches(matches)

        logger.info(f"\n📊 RESUMO FINAL:")
        logger.info(f"  ➕ Novas partidas: {stats['new']}")
        logger.info(f"  🔄 Partidas atualizadas: {stats['updated']}")
        logger.info(f"  ⏭️  Partidas puladas: {stats['skipped']}")
        logger.info(f"  ❌ Erros: {stats['errors']}")
