"""
Modelos de dados do sistema
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from .config import BetStatus


@dataclass
class BetResult:
    """Resultado de uma aposta processada"""

    bet_id: int
    actual_value: float
    won: bool
    bet_status: BetStatus
    processing_time: float = 0.0


@dataclass
class GameStats:
    """Estatísticas de um jogo"""

    total_kills: Optional[int] = None
    total_barons: Optional[int] = None
    total_dragons: Optional[int] = None
    total_towers: Optional[int] = None
    total_inhibitors: Optional[int] = None
    game_duration: Optional[int] = None


@dataclass
class TeamInfo:
    """Informação de times"""

    home_name: str
    away_name: str
    match_date: datetime


@dataclass
class PendingBetAnalysis:
    """Análise de aposta pendente"""

    bet_id: int
    event_id: str
    market_type: str
    selection: str
    created_at: datetime
    match_date: Optional[datetime]
    days_since_match: Optional[int]
    status: str  # 'future', 'recent', 'old'
