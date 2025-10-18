🎯 Separação de Responsabilidades:
1. orchestrator.py - Fluxo Principal
python- Busca eventos novos
- Coordena análise
- Salva apostas
- (Opcionalmente) notifica
2. database.py - Acesso aos Bancos
python- lol_bets.db: events, bets, teams
- Queries de leitura/escrita
- Gerencia transações
3. services/odds_fetcher.py - Busca Odds
python- Conecta em lol_odds.db
- Busca eventos disponíveis
- Busca linhas de apostas por mercado
- Busca info dos times/eventos
4. services/stats_calculator.py - Estatísticas
python- Conecta em lol_history.db
- Busca histórico dos times
- Calcula médias/probabilidades
- Substitui o CSV de players
5. services/bet_analyzer.py - Análise e ROI
python- Recebe: odds + histórico
- Calcula: probabilidade, fair_odds, ROI
- Retorna: apostas com value
6. services/notification_service.py - Telegram (Opcional)
python- Formata mensagens
- Envia notificações
- Pode ser desabilitado facilmente