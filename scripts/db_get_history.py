import asyncio
import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.get_history.orchestrator import HistoryOrchestrator
from src.shared.utils.logging_config import setup_logging

setup_logging("lol_history", log_dir=Path(__file__).parent.parent / "logs")


async def main():
    orchestrator = HistoryOrchestrator()
    
    # Padrão: últimos 3 dias (atualização diária)
    await orchestrator.run(days_back=3)


if __name__ == "__main__":
    asyncio.run(main())