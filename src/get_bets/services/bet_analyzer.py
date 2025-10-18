import logging
from typing import Dict, List

logger = logging.getLogger("lol_bets")


class BetAnalyzer:
    """Analisa odds e identifica apostas com valor usando estratégia configurada"""
    
    def __init__(self, strategy):
        self.strategy = strategy
    
    def analyze_event(self, event_id: str, stake: float = 1.0) -> List[Dict]:
        """
        Analisa um evento e retorna apostas com valor.
        
        Returns:
            Lista de apostas que atendem o critério de ROI mínimo
        """
        try:
            # Preparar dados do evento para a estratégia
            event_data = {
                "event_id": event_id,
                "stake": stake
                # Outros dados serão adicionados pela estratégia conforme necessário
            }
            
            # Usar a estratégia para analisar o evento
            good_bets = self.strategy.analyze(event_data, stake)
            
            logger.info(f"📊 Estratégia encontrou {len(good_bets)} apostas para evento {event_id}")
            
            # Converter para o formato esperado pelo orchestrator se necessário
            # (A estratégia já retorna no formato correto: List[Dict])
            return good_bets
            
        except Exception as e:
            logger.error(f"❌ Erro ao analisar evento {event_id}: {e}")
            return []