import logging
from datetime import datetime

from src.get_bets.models.bet import Bet
from src.shared.services.telegram_notifier import TelegramNotifier

logger = logging.getLogger("lol_bets")


class NotificationService:
    """Envia notificações sobre apostas encontradas"""
    
    def __init__(self):
        self.telegram = TelegramNotifier()
    
    def notify_bets(self, bets: list[Bet], event_info: dict):
        """Notifica sobre apostas de um evento, agrupadas"""
        if not bets:
            return
        
        try:
            message = self._format_message(bets, event_info)
            success = self.telegram.send_message(message, parse_mode="Markdown")
            
            if success:
                logger.info(f"📤 Notificação enviada: {len(bets)} apostas")
            else:
                logger.warning("⚠️ Falha ao enviar notificação")
        
        except Exception as e:
            logger.error(f"❌ Erro ao notificar: {e}")
    
    def _format_message(self, bets: list[Bet], event_info: dict) -> str:
        """Formata mensagem agrupada de apostas"""
        home_team = event_info.get("home_team_name", "Unknown")
        away_team = event_info.get("away_team_name", "Unknown")
        league = event_info.get("league_name", "Unknown")
        match_date = event_info.get("match_date", "Unknown")
        
        # Formatar data
        try:
            dt = datetime.strptime(match_date, "%Y-%m-%d %H:%M:%S")
            formatted_date = dt.strftime("%d/%m/%Y às %H:%M")
        except:
            formatted_date = match_date
        
        # Cabeçalho
        message = f"🎯 *Nova Aposta Encontrada!*\n\n"
        message += f"🏆 *Liga:* {league}\n"
        message += f"⚔️ *Partida:* {home_team} vs {away_team}\n"
        message += f"📅 *Data:* {formatted_date}\n\n"
        
        # Agrupar por mapa
        by_map = {}
        for bet in bets:
            map_key = bet.market_type
            if map_key not in by_map:
                by_map[map_key] = []
            by_map[map_key].append(bet)
        
        # Apostas
        for market, market_bets in by_map.items():
            message += f"🗺️ *{market}:*\n"
            for bet in market_bets:
                message += f"   ✅ {bet.selection} {bet.line}\n"
                message += f"   💰 Odds: {bet.odds} | Fair: {bet.fair_odds:.2f} | ROI: {bet.roi_average:.1f}%\n"
            message += f"\n💵 *Stake:* {bet.stake} unidade(s) por aposta\n\n"
        
        message += "#LoL #Bet365 #EV+"
        
        return message