import logging
from datetime import datetime

from src.get_bets.database import BetsDatabase
from src.get_bets.models.bet import Event
from src.get_bets.services.bet_analyzer import BetAnalyzer
from src.get_bets.services.notification_service import NotificationService
from src.get_bets.services.odds_fetcher import OddsFetcher
from src.get_bets.strategies import get_strategy

logger = logging.getLogger("lol_bets")


class BetsOrchestrator:
    def __init__(self, strategy: str = "statistical", min_roi: float = 10.0, odds_db_path: str = None):
        self.db = BetsDatabase()
        self.odds_fetcher = OddsFetcher()
        self.notification = NotificationService()
        
        self.strategy_name = strategy  # ✅ ADICIONAR ESTA LINHA
        
        # Configurar estratégia com odds_db_path para a estratégia estatística
        self.strategy = get_strategy(strategy, min_roi=min_roi, odds_db_path=odds_db_path)
        self.bet_analyzer = BetAnalyzer(self.strategy)
        
        logger.info(f"🎯 Estratégia: {self.strategy.get_description()}")

    async def run(self, stake: float = 1.0):
        """Executa o fluxo completo de análise de apostas"""
        start_time = datetime.now()
        logger.info("🚀 Iniciando análise de apostas")
        
        try:
            # 1. Buscar eventos disponíveis
            available_events = self.odds_fetcher.get_available_events()
            analyzed_events = self.db.get_analyzed_events()
            new_events = set(available_events) - analyzed_events
            
            if not new_events:
                logger.info("✅ Todos os eventos já foram analisados")
                return
            
            logger.info(f"📋 {len(new_events)} eventos novos para analisar")
            
            # 2. Analisar cada evento
            total_bets = 0
            
            for i, event_id in enumerate(new_events, 1):
                logger.info(f"\n⚡ [{i}/{len(new_events)}] Analisando evento {event_id}")
                
                # Buscar info do evento
                event_info = self.odds_fetcher.get_event_info(event_id)
                if not event_info:
                    logger.warning(f"   ⚠️ Evento {event_id} não encontrado")
                    continue
                
                # Salvar evento no banco
                self._save_event(event_info)
                
                # Analisar apostas
                good_bets = self.bet_analyzer.analyze_event(event_id, stake)
                
                if good_bets:
                    logger.info(f"   ✅ {len(good_bets)} apostas encontradas")
                    
                    # Salvar apostas
                    saved_count = self._save_bets(good_bets)
                    
                    # No método run, onde chama a notificação:
                    if saved_count > 0:
                        # Preparar apostas para notificação
                        bets_to_notify = []
                        for bet in good_bets[:saved_count]:
                            # Criar objeto simples com os atributos necessários
                            bet_obj = type('SimpleBet', (), {
                                'event_id': bet['event_id'],
                                'market_type': bet['market_name'],  # Usar market_type
                                'selection': bet['selection_line'],  # Usar selection
                                'odds': bet['house_odds'],
                                'line': bet['handicap'],
                                'roi_average': bet['roi_average'],
                                'fair_odds': bet['fair_odds'],
                                'stake': stake
                            })()
                            bets_to_notify.append(bet_obj)
                        
                        # Notificar
                        self.notification.notify_bets(bets_to_notify, event_info)
                        total_bets += saved_count
                else:
                    logger.info(f"   ❌ Nenhuma aposta com valor")
            
            # 3. Resumo
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"\n✅ Análise completa em {elapsed:.1f}s")
            logger.info(f"🎯 Total: {total_bets} apostas salvas")
            
            # 4. Mostrar estatísticas
            self.show_stats()
        
        except Exception as e:
            logger.error(f"❌ Erro durante análise: {e}", exc_info=True)
            raise

    def _save_event(self, event_info: dict):
        """Salva evento no banco"""
        # Verificar se já existe
        existing = self.db.get_event(event_info["event_id"])
        if existing:
            return
        
        # Criar ou buscar times
        home_team_id = self.db.get_or_create_team(
            str(event_info["home_team_id"]),
            event_info["home_team_name"]
        )
        away_team_id = self.db.get_or_create_team(
            str(event_info["away_team_id"]),
            event_info["away_team_name"]
        )
        
        # Criar evento
        event = Event(
            event_id=event_info["event_id"],
            league_name=event_info["league_name"],
            match_date=event_info["match_date"],
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            home_team_name=event_info["home_team_name"],
            away_team_name=event_info["away_team_name"],
            FI=event_info.get("FI"),
            bet365_key=event_info.get("bet365_key"),
            match_timestamp=event_info.get("match_timestamp"),
        )
        
        self.db.insert_event(event)

    def _save_bets(self, bets: list) -> int:
        """Salva apostas no banco, retorna quantas foram salvas"""
        saved_count = 0
        
        for bet in bets:
            # Verificar duplicata - usar formato da estratégia
            if self.db.bet_exists(
                bet["event_id"], 
                bet["market_name"],  
                bet["selection_line"],  
                bet["handicap"]
            ):
                logger.debug(f"   ⏭️ Aposta duplicada: {bet['selection_line']} {bet['handicap']}")
                continue
            
            # Salvar
            self._save_bet_to_db(bet)
            saved_count += 1
        
        return saved_count

    def _save_bet_to_db(self, bet: dict):
        """Salva uma aposta individual no banco de dados"""
        try:
            logger.info(f"💾 Salvando aposta: {bet['selection_line']} {bet['handicap']} - ROI: {bet['roi_average']:.1f}%")
            
            # Converter formato da estratégia para formato do banco
            bet_data = {
                'event_id': bet['event_id'],
                'market_type': bet['market_name'],
                'selection': bet['selection_line'],
                'odds': bet['house_odds'],
                'line': str(bet['handicap']),
                'roi_average': bet['roi_average'],
                'fair_odds': bet['fair_odds'],
                'stake': 1.0,
                'potential_win': (bet['house_odds'] - 1) * 1.0,
                'odds_type': bet['odds_type'],
                'map_number': bet['map_number'],
                'actual_value': bet.get('actual_value'),
                'strategy': self.strategy_name  # ✅ ADICIONAR ESTA LINHA
            }
            
            # Inserir no banco
            bet_id = self.db.insert_bet(bet_data)
            
            if bet_id:
                logger.info(f"✅ Aposta salva com ID: {bet_id}")
            else:
                logger.error(f"❌ Falha ao salvar aposta")
                
        except Exception as e:
            logger.error(f"❌ Erro ao salvar aposta: {e}")


    def show_stats(self):
        """Mostra estatísticas gerais"""
        stats = self.db.get_stats()
        
        if not stats:
            logger.info("\n📊 Nenhuma estatística disponível ainda")
            return
        
        logger.info("\n📊 ESTATÍSTICAS GERAIS:")
        logger.info(f"   Total de apostas: {stats['total_bets']}")
        logger.info(f"   Pendentes: {stats['pending_bets']}")
        logger.info(f"   Ganhas: {stats['won_bets']}")
        logger.info(f"   Perdidas: {stats['lost_bets']}")
        
        if stats['total_stake'] > 0:
            logger.info(f"   Stake total: {stats['total_stake']:.2f}")
            logger.info(f"   ROI: {stats['roi']:.1f}%")
        
        if stats['won_bets'] + stats['lost_bets'] > 0:
            logger.info(f"   Win rate: {stats['win_rate']:.1f}%")