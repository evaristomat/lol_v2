# 📚 Documentação do Módulo `get_odds`

## 🎯 Visão Geral

O módulo `get_odds` é responsável por coletar, armazenar e gerenciar odds de jogos de League of Legends da API Bet365. Ele busca eventos futuros, coleta suas odds em tempo real e mantém um banco de dados atualizado.

---

## 📁 Estrutura de Arquivos

```
src/get_odds/
├── database.py              # Schema específico do módulo
├── orchestrator.py          # Coordena todo o fluxo de execução
└── services/
    ├── event_service.py     # Gerencia eventos (jogos)
    ├── odds_service.py      # Gerencia coleta de odds
    └── dashboard_service.py # Estatísticas e limpeza
```

---

## 🗄️ `database.py` - OddsDatabase

### Propósito
Define o schema do banco de dados SQLite específico para odds, herdando funcionalidades da `BaseDatabase`.

### Tabelas

#### **`teams`**
Armazena informações dos times.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID interno auto-incremento |
| `team_id` | TEXT UNIQUE | ID do time na API Bet365 |
| `name` | TEXT | Nome do time |
| `region` | TEXT | Região/país do time |
| `created_at` | TEXT | Data de criação |
| `updated_at` | TEXT | Data de atualização |

#### **`events`**
Armazena jogos/partidas.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID interno auto-incremento |
| `event_id` | TEXT UNIQUE | ID do evento na API Bet365 |
| `FI` | TEXT | ID alternativo Bet365 |
| `bet365_key` | TEXT | Chave usada na URL da Bet365 |
| `home_team_id` | INTEGER FK | Referência para `teams.id` (mandante) |
| `away_team_id` | INTEGER FK | Referência para `teams.id` (visitante) |
| `league_name` | TEXT | Nome da liga/campeonato |
| `match_date` | TEXT | Data/hora formatada do jogo |
| `match_timestamp` | INTEGER | Unix timestamp do jogo |
| `status` | TEXT | Status: 'upcoming', 'live', 'finished' |
| `created_at` | TEXT | Data de criação |
| `updated_at` | TEXT | Data de atualização |

#### **`current_odds`**
Armazena as odds atuais dos eventos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID interno auto-incremento |
| `event_id` | TEXT FK | Referência para `events.event_id` |
| `odds_type` | TEXT | Tipo: 'main', 'map_1', 'map_2', 'player' |
| `market_type` | TEXT | Nome do mercado (ex: "Match Lines", "Total Kills") |
| `selection` | TEXT | Seleção específica (ex: "Over", "Team 1", "Bwipo") |
| `odds` | REAL | Valor decimal da odd (ex: 1.83) |
| `line` | TEXT | Linha do handicap (ex: "+10.5", "-1.5") |
| `map_number` | INTEGER | Número do mapa (1, 2, NULL para main) |
| `updated_at` | TEXT | Timestamp da última atualização |
| `raw_data` | TEXT | JSON completo da odd original |

### Índices
- `idx_events_FI`: Busca rápida por FI
- `idx_current_odds_event`: Busca por evento
- `idx_current_odds_market`: Busca por tipo de mercado
- `idx_current_odds_map`: Busca por número do mapa

### Migração Automática
```python
# Adiciona colunas automaticamente se banco já existir
if 'FI' not in columns and columns:
    conn.execute("ALTER TABLE events ADD COLUMN FI TEXT")
```

---

## 🎮 `services/event_service.py` - EventService

### Propósito
Gerencia o ciclo de vida dos eventos: busca na API, detecta duplicatas, salva no banco.

### Métodos Principais

#### `fetch_upcoming_events(days_ahead: int = 10) -> List[Dict]`
Busca eventos futuros de LoL da API Bet365.

**Parâmetros:**
- `days_ahead`: Quantos dias à frente buscar (padrão: 10)

**Processo:**
1. Loop pelos próximos N dias
2. Chama API `upcoming()` para cada dia
3. Filtra apenas eventos de LoL usando `is_lol_event()`
4. Retorna lista de eventos válidos

**Exemplo de log:**
```
📅 Buscando eventos para 2025-10-16
   ✅ 8 jogos de LoL encontrados
```

---

#### `save_events(events: List[Dict]) -> Dict[str, int]`
Salva eventos no banco, detectando e prevenindo duplicatas.

**Lógica de Detecção de Duplicatas:**

```python
# 1. Verifica se event_id já existe
if event_id exists:
    → Skip (já está no banco)

# 2. Busca jogo duplicado por times + timestamp próximo (±24h)
if same_teams AND timestamp_diff < 24h:
    → DELETE odds antigas
    → UPDATE event_id para o novo
    → UPDATE horário/data/liga
    → Log: "🔄 Evento atualizado"

# 3. Caso contrário, insere novo evento
else:
    → INSERT novo registro
    → Log: "✅ Novo evento"
```

**Retorno:**
```python
{
    "new": 5,        # Eventos novos inseridos
    "existing": 10,  # Eventos já existentes (skip)
    "updated": 2     # Eventos remarcados (event_id atualizado)
}
```

---

#### `_get_or_create_team(conn, team_id: str, team_name: str) -> int`
Busca ou cria um time, retornando o **ID interno** (não o team_id da API).

**Por que retornar `id` em vez de `team_id`?**
- `teams.id` é INTEGER (primary key, auto-incremento)
- `teams.team_id` é TEXT (ID da API Bet365)
- Foreign keys em `events` apontam para `teams.id`

---

## 💰 `services/odds_service.py` - OddsService

### Propósito
Busca e armazena odds da API Bet365, filtrando apenas mercados relevantes.

### Métodos Principais

#### `fetch_and_save_odds(hours_old_threshold: int = 2, batch_size: int = 10) -> int`
Busca e salva odds em lotes, apenas para eventos desatualizados.

**Parâmetros:**
- `hours_old_threshold`: Recoleta odds mais antigas que N horas (padrão: 2)
- `batch_size`: Quantos eventos processar por lote (padrão: 10)

**Processo:**
1. Busca eventos que precisam de atualização
2. Divide em lotes para performance
3. Processa lotes em paralelo (semaphore = 10)
4. Aguarda 0.5s entre lotes

**Query de Seleção:**
```sql
SELECT e.event_id, ht.name, at.name
FROM events e
WHERE e.status = 'upcoming'
AND (
    co.event_id IS NULL  -- Sem odds ainda
    OR 
    datetime(co.updated_at) < datetime('now', '-2 hours')  -- Odds antigas
)
```

---

#### `_save_odds_data(event_id: str, odds_data: Dict)`
Processa e salva odds, aplicando filtros de mercados.

**Atualiza evento com metadados:**
```python
UPDATE events 
SET FI = ?, bet365_key = ? 
WHERE event_id = ?
```

**Processamento:**
1. **Main**: Apenas `match_lines` (vencedor da partida)
2. **Map 1/2**: Handicaps, towers, dragons, totals, duration
3. **Player**: Total kills, deaths, assists

---

#### Filtros de Mercados

**Main (match_lines apenas):**
```python
allowed_markets = {"match_lines"}
```

**Por Mapa (map_1, map_2):**
```python
allowed_markets = {
    "map_X_handicaps",           # Kill Handicap
    "map_X_tower_handicap",      # Torre Handicap
    "map_X_dragon_handicap",     # Dragão Handicap
    "map_X_totals",              # Total Kills/Towers/Dragons/Barons/Inhibitors
    "map_X_map_duration_2_way"   # Duração do mapa
}
```

**Player Odds:**
```python
allowed_markets = {
    "map_X_player_total_kills",
    "map_X_player_total_deaths",
    "map_X_player_total_assists"
}
```

**Mercados IGNORADOS:**
- `first_team_to` (First Blood, etc)
- `race_to_#_kills`
- `both_teams_to`
- `first_dragon_to_spawn`
- `either_team_to`
- `largest_multi_kill`
- `total_kills_odd_even`

---

#### `_extract_map_number(section: str) -> Optional[int]`
Extrai número do mapa de strings como "map_1", "map_2".

```python
"map_1" → 1
"map_2" → 2
"main"  → None
"map_1_player_total_kills" → 1
```

---

#### `_save_single_odd(...) -> int`
Salva uma odd individual no banco.

**Estrutura da odd:**
```python
{
    "id": "1869681808",
    "odds": "1.20",
    "header": "1",          # Time 1, Time 2, Over, Under
    "name": "To Win",
    "handicap": "+10.5"     # Linha (se aplicável)
}
```

**Processamento:**
```python
selection = f"{header} {name}".strip()  # "1 To Win" → "1 To Win"
odds = float(odd.get("odds", 0))
line = odd.get("handicap", "")
```

---

## 📊 `services/dashboard_service.py` - DashboardService

### Propósito
Gera estatísticas, relatórios e gerencia limpeza de dados antigos.

### Métodos Principais

#### `generate() -> str`
Gera dashboard textual com estatísticas.

**Informações exibidas:**
```
📈 ESTATÍSTICAS GERAIS:
  Total de Eventos: 18
  Total de Times: 20
  Odds Atuais: 974
  Player Odds: 714
  Tamanho do Banco: 0.32 MB

🎮 PRÓXIMOS EVENTOS:
  Eventos Futuros: 18
  Próximas 24h: 2

📊 COBERTURA DE ODDS:
  Eventos com Odds: 18/18 (100.0%)
  Eventos com Player Odds: 10

⏰ Última Atualização: 2025-10-16 14:52:08
```

---

#### `save_to_file(content: str, report_dir: str) -> Path`
Salva dashboard em arquivo `.txt` com timestamp.

**Exemplo:**
```
reports/dashboard_20251016_110118.txt
```

---

#### `cleanup_old_data(days_keep: int = 30) -> dict`
Remove eventos antigos e times órfãos.

**Processo:**
1. Deleta eventos com `match_timestamp` > 30 dias no passado
2. Deleta times sem nenhum evento associado
3. Executa `VACUUM` para compactar banco

**Retorno:**
```python
{
    "deleted_events": 45,
    "deleted_teams": 12
}
```

---

## 🎭 `orchestrator.py` - OddsOrchestrator

### Propósito
Coordena a execução de todas as fases do processo de coleta de odds.

### Fluxo de Execução

```python
async def run():
    1. _fetch_events()      # Busca e salva eventos
    2. _update_odds()       # Coleta odds dos eventos
    3. _weekly_cleanup()    # Limpeza (apenas segunda-feira)
    4. _show_dashboard()    # Exibe estatísticas
```

### Fases Detalhadas

#### **FASE 1: Buscar Eventos**
```python
async def _fetch_events(self):
    events = await event_service.fetch_upcoming_events(days_ahead=10)
    stats = event_service.save_events(events)
    # Log: "✅ Fase 1 concluída - X novos eventos"
```

#### **FASE 2: Atualizar Odds**
```python
async def _update_odds(self):
    odds_collected = await odds_service.fetch_and_save_odds(
        hours_old_threshold=2,
        batch_size=10
    )
    # Log: "✅ Fase 2 concluída - X eventos com odds atualizadas"
```

#### **FASE 3: Limpeza Semanal** (apenas segunda-feira)
```python
def _weekly_cleanup(self):
    if datetime.now().weekday() == 0:  # Segunda
        result = dashboard_service.cleanup_old_data(days_keep=30)
```

#### **FASE 4: Dashboard**
```python
def _show_dashboard(self):
    dashboard = dashboard_service.generate()
    print(dashboard)
    dashboard_file = dashboard_service.save_to_file(dashboard)
```

---

## 🔧 Componentes Compartilhados

### `RateLimiter` (shared/services)
Controla taxa de requisições para respeitar limites da API.

**Configuração:**
- Máximo: 3500 requisições
- Janela: 3600 segundos (1 hora)

**Funcionamento:**
```python
await rate_limiter.acquire()  # Espera se limite atingido
# Faz requisição
```

### `BaseDatabase` (shared/core)
Classe base com context manager para conexões SQLite.

```python
with db.get_connection() as conn:
    conn.execute(...)  # Auto-commit
    # Auto-rollback em caso de erro
    # Auto-close ao sair
```

### `is_lol_event()` (shared/utils/validators)
Filtra apenas eventos de League of Legends.

**Critérios:**
- Nome da liga começa com "LOL -"
- Não contém keywords de outros jogos (CS:GO, DOTA, etc)

---

## 🚀 Script de Entrada

### `scripts/db_get_odds.py`
Ponto de entrada minimal (17 linhas).

```python
from src.get_odds.orchestrator import OddsOrchestrator
from src.shared.utils.logging_config import setup_logging

setup_logging("lol_odds", log_dir="logs")

async def main():
    orchestrator = OddsOrchestrator()
    await orchestrator.run()

asyncio.run(main())
```

---

## 📈 Fluxo Completo de Dados

```
1. API Bet365 (upcoming)
   ↓
2. EventService.fetch_upcoming_events()
   ↓ Filtra LoL
3. EventService.save_events()
   ↓ Detecta duplicatas
4. Database (events, teams)
   ↓
5. OddsService._get_events_to_update()
   ↓ Seleciona desatualizados
6. API Bet365 (prematch)
   ↓
7. OddsService._save_odds_data()
   ↓ Filtra mercados
8. Database (current_odds)
   ↓
9. DashboardService.generate()
   ↓
10. Console + arquivo .txt
```

---

## ⚙️ Configurações Importantes

### Parâmetros Ajustáveis

```python
# Busca eventos
days_ahead = 10              # Próximos 10 dias

# Atualização de odds
hours_old_threshold = 2      # Recoleta a cada 2 horas
batch_size = 10              # 10 eventos por lote
semaphore = 10               # 10 requisições paralelas

# Rate limiting
max_requests = 3500          # Por hora
time_window = 3600           # 1 hora

# Limpeza
days_keep = 30               # Mantém últimos 30 dias
cleanup_day = 0              # Segunda-feira (0-6)

# Cache
cache_ttl = 3600             # 1 hora (segundos / 3600)
max_cache_size = 1000        # Máximo de items
```

---

## 🎯 Casos de Uso

### 1. Execução Manual
```bash
python scripts/db_get_odds.py
```

### 2. Execução Automatizada (GitHub Actions)
```yaml
- cron: '0 9 * * *'   # 6:00 AM Brasília
- cron: '0 15 * * *'  # 12:00 PM Brasília  
- cron: '0 21 * * *'  # 6:00 PM Brasília
```

### 3. Forçar Recoleta de Odds
```bash
# Deletar odds antigas para forçar recoleta
sqlite3 data/lol_odds.db "DELETE FROM current_odds"
python scripts/db_get_odds.py
```

---

## 🐛 Troubleshooting

### "no such column: FI"
**Causa:** Banco criado antes da adição das novas colunas.
**Solução:**
```bash
rm data/lol_odds.db
python scripts/db_get_odds.py
```

### "FOREIGN KEY constraint failed"
**Causa:** `_get_or_create_team()` retornando `team_id` em vez de `id`.
**Verificar:** Método deve retornar `cursor.lastrowid` após INSERT.

### Odds não atualizando
**Verificar:**
1. `hours_old_threshold` muito alto?
2. Query `_get_events_to_update()` usando joins corretos?
3. Eventos com `status != 'upcoming'`?

---

## 📝 Melhorias Futuras

- [ ] Histórico de mudanças de odds (tabela `odds_history`)
- [ ] Alertas para odds de valor alto
- [ ] API GraphQL para consultas
- [ ] Dashboard web interativo
- [ ] Detecção de padrões em mudanças de odds
- [ ] Integração com outros sites de apostas

---

## 📞 Suporte

Para dúvidas sobre o módulo, consulte:
- Logs em `logs/lol_odds_*.log`
- Dashboard em `reports/dashboard_*.txt`
- Estrutura do banco: `sqlite3 data/lol_odds.db .schema`
