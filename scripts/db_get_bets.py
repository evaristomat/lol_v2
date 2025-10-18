# scripts/db_get_bets.py
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Configurar logging mais detalhado
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)

from src.get_bets.orchestrator import BetsOrchestrator


async def main():
    base_dir = Path(__file__).parent.parent
    
    # CORREÇÃO: O banco está em data/lol_odds.db, não data/dbs/
    odds_db_path = base_dir / "data" / "lol_odds.db"
    
    print(f"🎯 Trabalhando no diretório: {base_dir}")
    print(f"📁 Banco de odds: {odds_db_path}")
    print(f"📁 Existe banco: {odds_db_path.exists()}")
    
    if not odds_db_path.exists():
        print("❌ ATENÇÃO: O banco de odds não existe!")
        print("💡 Execute o coletor de odds primeiro")
        return

    orchestrator = BetsOrchestrator(
        strategy="statistical", 
        min_roi=15.0,
        odds_db_path=str(odds_db_path)
    )
    
    await orchestrator.run(stake=1.0)

if __name__ == "__main__":
    asyncio.run(main())