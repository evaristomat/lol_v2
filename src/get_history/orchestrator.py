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

    async def run(self, days_back: int = 2):
        start_time = datetime.now()
        logger.info(f"🚀 Iniciando atualização - últimos {days_back} dias")

        try:
            stats = await self.match_service.fetch_and_process_matches(days_back)

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"✅ Completo em {elapsed:.1f}s - "
                f"Novas: {stats['new']}, Atualizadas: {stats['updated']}, "
                f"Erros: {stats['errors']}"
            )

            return stats

        except Exception as e:
            logger.error(f"❌ Erro: {e}", exc_info=True)
            raise
        finally:
            await self.client.close()
