"""
Configurações e constantes do sistema
"""

import logging
from dataclasses import dataclass
from enum import Enum

from colorama import Fore, Style

# Mapeamento de nomes de times para LCKC e outras ligas
TEAM_NAME_MAPPINGS = {
    "BNK FearX": "BNK FEARX Youth",
    "BNK FearX.Y": "BNK FEARX Youth",
    "DN Freecs.Ch": "DN Freecs Challengers",
    "DRX.Ch": "DRX Challengers",
    "Dplus KIA.Ch": "Dplus KIA Challengers",
    "Gen.G.GA": "Gen.G Global Academy",
    "Hanwha Life Esports.Ch": "Hanwha Life Esports Challengers",
    "KT Rolster.Ch": "KT Rolster Challengers",
    "Nongshim.EA": "Nongshim Esports Academy",
    "T1.EA": "T1 Esports Academy",
}


@dataclass
class Config:
    """Configuração centralizada do sistema"""

    # Paths
    DB_BETS = "data/lol_bets.db"
    DB_HISTORY = "data/lol_history.db"
    CSV_MATCHES = "data/database/database.csv"

    # Performance
    BATCH_SIZE = 100
    MAX_WORKERS = 4
    CACHE_SIZE = 128
    CONNECTION_TIMEOUT = 30

    # Business rules
    PARTICIPANT_ID_RANGE = (1, 10)  # Para players
    TEAM_PARTICIPANT_IDS = [100, 200]  # Para times

    # Melhorias de matching
    DATE_TOLERANCE_DAYS = 1  # Tolerância de +/- 1 dia para datas
    TEAM_NAME_MAPPINGS = TEAM_NAME_MAPPINGS


class BetStatus(Enum):
    """Estados possíveis de uma aposta"""

    PENDING = "pending"
    TO_VERIFY = "to_verify"
    WON = "won"
    LOST = "lost"
    VOID = "void"
    NOT_FOUND = "not_found"


class MarketType(Enum):
    """Tipos de mercado"""

    TOTAL_KILLS = "Total Kills"
    TOTAL_BARONS = "Total Barons"
    TOTAL_DRAGONS = "Total Dragons"
    TOTAL_INHIBITORS = "Total Inhibitors"
    TOTAL_TOWERS = "Total Towers"
    GAME_DURATION = "Game Duration"
    PLAYER_KILLS = "Player Kills"
    PLAYER_ASSISTS = "Player Assists"
    PLAYER_DEATHS = "Player Deaths"


class SelectionType(Enum):
    """Tipos de seleção"""

    OVER = "Over"
    UNDER = "Under"


class SimpleColoredFormatter(logging.Formatter):
    """Formatter simplificado e robusto"""

    COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        message = super().format(record)
        color = self.COLORS.get(record.levelno, "")
        return f"{color}{message}{Style.RESET_ALL}"


def setup_logger():
    """Configura logger de forma robusta"""
    logger = logging.getLogger(__name__)

    # Evitar duplicação de handlers
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()

    formatter = SimpleColoredFormatter(
        "%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
