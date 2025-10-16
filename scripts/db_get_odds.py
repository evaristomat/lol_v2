import asyncio
import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.get_odds.orchestrator import OddsOrchestrator
from src.shared.utils.logging_config import setup_logging

setup_logging("lol_odds", log_dir=Path(__file__).parent.parent / "logs")


async def main():
    orchestrator = OddsOrchestrator()
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())
