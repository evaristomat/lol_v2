# src/get_bets/strategies/strategies.py
import logging
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger("lol_bets")


class BettingStrategy(ABC):
    @abstractmethod
    def analyze(self, event_data: Dict, stake: float) -> List[Dict]:
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        pass

    # Métodos utilitários do seu código original
    @staticmethod
    def _extract_side(selection_name: str) -> Optional[str]:
        s = (selection_name or "").strip()
        if not s:
            return None
        head = s.split(maxsplit=1)[0].casefold()
        if head == "over":
            return "over"
        if head == "under":
            return "under"
        return None

    @staticmethod
    def _market_stat_label(market_name: str) -> str:
        """Retorna rótulo curto do stat a partir do market_name."""
        s = (market_name or "").lower()
        if "player total kills" in s:
            return "Kills"
        if "player total deaths" in s:
            return "Deaths"
        if "player total assists" in s:
            return "Assists"
        if "totals" in s:
            return "Totals"
        return "Market"

    @staticmethod
    def _extract_player(selection_name: str, candidates: List[str]) -> Optional[str]:
        """
        Encontra o nome do jogador dentro do selection_name (ex.: 'Over SkewMond').
        Tenta word-boundary e depois substring, para robustez.
        """
        low = str(selection_name).lower()
        for c in sorted(candidates, key=len, reverse=True):
            if f" {c.lower()} " in f" {low} ":
                return c
        for c in sorted(candidates, key=len, reverse=True):
            if c.lower() in low:
                return c
        return None

    @staticmethod
    def _implied_prob(odds: float) -> float:
        p = 1.0 / max(float(odds), 1e-12)
        return float(np.clip(p, 1e-6, 1 - 1e-6))

    @staticmethod
    def _fair_from_p(p: float) -> float:
        p = float(np.clip(p, 1e-6, 1 - 1e-6))
        return 1.0 / p

    @staticmethod
    def _posterior(p_prior: float, p_like: float, w_prior: float = 0.5) -> float:
        p_prior = float(np.clip(p_prior, 1e-6, 1 - 1e-6))
        p_like = float(np.clip(p_like, 1e-6, 1 - 1e-6))
        return w_prior * p_prior + (1.0 - w_prior) * p_like

    @staticmethod
    def _ev_percent(p_real: float, odds: float) -> float:
        p_real = float(np.clip(p_real, 1e-6, 1 - 1e-6))
        return (p_real * float(odds) - 1.0) * 100.0

    @staticmethod
    def _calc_window_stats(
        values: np.ndarray, handicap: float, side: str
    ) -> Optional[dict]:
        if values is None or len(values) == 0:
            return None
        mean = float(np.mean(values))
        median = float(np.median(values))
        std = float(np.std(values, ddof=0))
        cv = float((std / mean * 100.0) if mean > 0 else 0.0)
        hit_rate = (
            float(np.mean(values > handicap))
            if side == "over"
            else float(np.mean(values < handicap))
        )
        # trend
        trend = 0.0
        if len(values) >= 20:
            recent = float(np.mean(values[:10]))
            older = float(np.mean(values[10:20]))
            trend = float(((recent - older) / older * 100.0) if older > 0 else 0.0)
        return dict(
            mean=mean, median=median, std=std, cv=cv, hit_rate=hit_rate, trend=trend
        )


class StatisticalStrategy(BettingStrategy):
    def __init__(self, min_roi: float = 10.0, odds_db_path: str = None, player_history_df: Optional[pd.DataFrame] = None):
        self.min_roi = min_roi
        self.odds_db_path = odds_db_path
        self.player_history_df = player_history_df
        
        logger.info(f"🔧 Configurando estratégia com ROI mínimo: {min_roi}%")
        logger.info(f"📁 Caminho do banco de odds: {odds_db_path}")
        
        if odds_db_path:
            try:
                from src.get_bets.services.roi_analyzer import ROIAnalyzer
                self.roi_analyzer = ROIAnalyzer(odds_db_path)
                logger.info("✅ ROIAnalyzer carregado com sucesso")
            except ImportError as e:
                self.roi_analyzer = None
                logger.warning(f"⚠️ ROIAnalyzer não disponível: {e}")
        else:
            self.roi_analyzer = None
            logger.warning("⚠️ Caminho do banco de odds não fornecido")
    
    def get_description(self) -> str:
        return f"Estratégia Estatística (ROI mínimo: {self.min_roi}%)"
    
    def analyze(self, event_data: Dict, stake: float) -> List[Dict]:
        """Implementa a lógica original de análise combinando players e totals"""
        event_id = event_data["event_id"]
        logger.info(f"🔍 Iniciando análise para evento {event_id}")
        
        good_bets = []
        
        # 1. Análise de Players (sua lógica de kills/deaths/assists)
        logger.info(f"🎯 Buscando apostas de players para evento {event_id}")
        player_bets = self._analyze_player_markets(event_data, stake)
        logger.info(f"📊 Encontradas {len(player_bets)} apostas de players")
        good_bets.extend(player_bets)
        
        # 2. Análise de Totals (sua lógica de over/under) - se ROIAnalyzer disponível
        logger.info(f"🎯 Buscando apostas de totals para evento {event_id}")
        total_bets = self._analyze_total_markets(event_data, stake)
        logger.info(f"📊 Encontradas {len(total_bets)} apostas de totals")
        good_bets.extend(total_bets)
        
        # Ordenar por ROI e limitar (mantendo seu limite original)
        good_bets.sort(key=lambda x: x["roi_average"], reverse=True)
        result = good_bets[:10]
        logger.info(f"✅ Análise finalizada: {len(result)} apostas válidas")
        
        return result
    
    def _load_player_history(self) -> bool:
        """Carrega o CSV de histórico de players com os paths corrigidos"""
        try:
            # PATHS CORRIGIDOS - baseado na estrutura que você mencionou
            base_dir = Path(__file__).resolve().parent.parent.parent.parent  # lol_api_v2/
            possible_paths = [
                base_dir / "data" / "database" / "database.csv",  # lol_api_v2/data/database/database.csv
                base_dir / "data" / "dbs" / "database.csv",       # lol_api_v2/data/dbs/database.csv (fallback)
                Path("data/database/database.csv"),               # Relativo ao working directory
                Path("../data/database/database.csv"),            # Um nível acima
            ]
            
            csv_path = None
            for path in possible_paths:
                logger.info(f"🔍 Procurando CSV em: {path}")
                if path.exists():
                    csv_path = path
                    logger.info(f"✅ CSV encontrado em: {path}")
                    break
            
            if not csv_path:
                logger.error("❌ CSV de players não encontrado em nenhum local")
                for path in possible_paths:
                    logger.info(f"   Tentado: {path} - Existe: {path.exists()}")
                return False

            logger.info(f"📖 Lendo CSV de players: {csv_path}")
            df = pd.read_csv(csv_path, low_memory=False)
            
            needed_cols = ["playername", "teamname", "date", "kills", "deaths", "assists"]
            if not all(c in df.columns for c in needed_cols):
                missing = [c for c in needed_cols if c not in df.columns]
                logger.error(f"❌ CSV não contém colunas necessárias: {missing}")
                logger.info(f"   Colunas disponíveis: {list(df.columns)}")
                return False

            logger.info("🔄 Convertendo tipos de dados...")
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            for c in ["kills", "deaths", "assists"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            self.player_history_df = df
            logger.info(f"✅ CSV carregado com sucesso: {len(df)} registros")
            logger.info(f"📊 Estatísticas do CSV:")
            logger.info(f"   Players únicos: {df['playername'].nunique()}")
            logger.info(f"   Times únicos: {df['teamname'].nunique()}")
            logger.info(f"   Período: {df['date'].min()} até {df['date'].max()}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao carregar CSV de players: {e}")
            return False

    def _get_player_values(
        self, player: str, team: Optional[str], stat: str, n: int = 50
    ) -> np.ndarray:
        """
        Últimos n valores do player para um stat. 
        Mesma lógica do seu código original.
        """
        if self.player_history_df is None or self.player_history_df.empty:
            logger.debug(f"📭 Histórico de players vazio para {player}")
            return np.array([])

        sub = self.player_history_df[self.player_history_df["playername"] == player]
        logger.debug(f"🔍 Buscando {player} - encontrados {len(sub)} registros")
        
        if team is not None:
            sub_team = sub[sub["teamname"] == team]
            logger.debug(f"   No time {team}: {len(sub_team)} registros")
        else:
            sub_team = pd.DataFrame(columns=sub.columns)
            
        if sub_team.empty:
            if not sub.empty:
                counts = sub.groupby("teamname").size().sort_values(ascending=False)
                if len(counts) > 0:
                    fallback = counts.index[0]
                    logger.debug(f"   Usando fallback: {fallback}")
                    sub_team = sub[sub["teamname"] == fallback]
                else:
                    sub_team = sub
                    
        sub_team = sub_team.dropna(subset=[stat]).sort_values("date", ascending=False)
        values = sub_team[stat].astype(float).values[:n]
        
        logger.debug(f"   Valores de {stat} para {player}: {len(values)} registros")
        if len(values) > 0:
            logger.debug(f"   Média: {np.mean(values):.2f}, Min: {np.min(values)}, Max: {np.max(values)}")
            
        return values

    def _analyze_player_markets(self, event_data: Dict, stake: float) -> List[Dict]:
        """Analisa odds de players usando a estrutura real do banco"""
        good_bets: List[Dict] = []
        event_id = event_data["event_id"]

        logger.info(f"🎮 Iniciando análise de players para evento {event_id}")

        # Garantir histórico carregado
        if self.player_history_df is None:
            logger.info("📥 Carregando histórico de players...")
            ok = self._load_player_history()
            if not ok:
                logger.error("❌ Falha ao carregar histórico de players")
                return good_bets

        # Buscar informações dos times do evento
        home_team = None
        away_team = None
        
        try:
            if self.odds_db_path:
                odds_db_path = Path(self.odds_db_path)
                conn = sqlite3.connect(str(odds_db_path))
                cursor = conn.cursor()
                
                # Buscar informações do evento na tabela events
                cursor.execute("""
                    SELECT t1.name, t2.name 
                    FROM events e
                    JOIN teams t1 ON e.home_team_id = t1.id
                    JOIN teams t2 ON e.away_team_id = t2.id
                    WHERE e.event_id = ?
                """, (event_id,))
                
                result = cursor.fetchone()
                if result:
                    home_team, away_team = result
                    logger.info(f"🏠 Time da casa: {home_team}")
                    logger.info(f"🚗 Time visitante: {away_team}")
                else:
                    logger.warning(f"⚠️ Evento {event_id} não encontrado na tabela events")
                
                conn.close()
        except Exception as e:
            logger.error(f"❌ Erro ao buscar informações do evento: {e}")

        # Buscar odds de players do banco
        if not self.odds_db_path:
            logger.error("❌ Caminho do banco de odds não configurado")
            return good_bets

        try:
            odds_db_path = Path(self.odds_db_path)
            logger.info(f"📊 Conectando ao banco de odds: {odds_db_path}")
            
            if not odds_db_path.exists():
                logger.error(f"❌ Banco de odds não encontrado: {odds_db_path}")
                return good_bets
                
            conn = sqlite3.connect(str(odds_db_path))
            
            # ✅ Query corrigida com os nomes REAIS das colunas
            q = """
                SELECT market_type, selection, line, odds
                FROM current_odds
                WHERE event_id = ?
                AND odds_type = 'player'
                AND market_type IN (
                    'Map 1 - Player Total Kills',
                    'Map 1 - Player Total Deaths', 
                    'Map 1 - Player Total Assists'
                )
            """
            logger.info(f"🔍 Buscando odds de players para evento {event_id}")
            odds_df = pd.read_sql_query(q, conn, params=[event_id])
            conn.close()
            
            logger.info(f"📈 Encontradas {len(odds_df)} odds de players brutas")

            if odds_df.empty:
                logger.info("📭 Nenhuma odd de player encontrada")
                return good_bets

            # Mapear mercado -> coluna do CSV
            stat_map = {
                "Map 1 - Player Total Kills": "kills",
                "Map 1 - Player Total Deaths": "deaths",
                "Map 1 - Player Total Assists": "assists",
            }

            # Parsing
            logger.info("🔄 Processando dados de odds...")
            
            # ✅ Converter line para numérico (pode ser string vazia ou número)
            odds_df["handicap"] = pd.to_numeric(odds_df["line"], errors='coerce')
            odds_df["odds_value"] = pd.to_numeric(odds_df["odds"], errors='coerce')
            odds_df["side"] = odds_df["selection"].apply(self._extract_side)
            odds_df["stat"] = odds_df["market_type"].map(stat_map)

            candidates = self.player_history_df["playername"].dropna().unique().tolist()
            logger.info(f"👥 {len(candidates)} players únicos no histórico")
            
            odds_df["player"] = odds_df["selection"].apply(
                lambda s: self._extract_player(s, candidates)
            )

            # Filtrar válidas
            dfv = odds_df[
                odds_df["handicap"].notna()
                & odds_df["odds_value"].notna()
                & odds_df["side"].notna()
                & odds_df["stat"].notna()
                & odds_df["player"].notna()
            ].copy()

            logger.info(f"✅ {len(dfv)} odds válidas após filtragem")

            if dfv.empty:
                return good_bets

            # Processar cada aposta de player
            logger.info("🧮 Calculando ROI para cada aposta...")
            for i, (_, row) in enumerate(dfv.iterrows()):
                player = row["player"]
                stat = row["stat"]
                side = row["side"]
                line = float(row["handicap"])
                odds = float(row["odds_value"])
                market_name = row["market_type"]
                selection_line = row["selection"]

                logger.debug(f"   [{i+1}/{len(dfv)}] Analisando {player} - {stat} {side} {line} @ {odds}")

                # Escolher time heurístico
                v_home = self._get_player_values(player, home_team, stat, n=1)
                v_away = self._get_player_values(player, away_team, stat, n=1)
                team_guess = home_team if len(v_home) >= len(v_away) else away_team
                logger.debug(f"      Time escolhido: {team_guess}")

                values = self._get_player_values(player, team_guess, stat, n=50)
                if len(values) < 20:
                    logger.debug(f"      ❌ Dados insuficientes: {len(values)} < 20")
                    continue

                logger.debug(f"      ✅ Dados suficientes: {len(values)} registros")

                l10 = values[:10]
                l20 = values[:20]
                st10 = self._calc_window_stats(l10, line, side)
                st20 = self._calc_window_stats(l20, line, side)
                
                if st10 is None or st20 is None:
                    logger.debug("      ❌ Não foi possível calcular estatísticas")
                    continue

                # MÉTODO ORIGINAL:
                p_prior = self._implied_prob(odds)
                p_like = 0.6 * st10["hit_rate"] + 0.4 * st20["hit_rate"]
                p_real = self._posterior(p_prior, p_like, w_prior=0.5)
                fair = self._fair_from_p(p_real)
                roi = (odds / fair - 1.0) * 100.0
                ev_pct = self._ev_percent(p_real, odds)

                logger.debug(f"      📊 Estatísticas: HR10={st10['hit_rate']:.3f}, HR20={st20['hit_rate']:.3f}")
                logger.debug(f"      🎯 Probabilidades: prior={p_prior:.3f}, like={p_like:.3f}, real={p_real:.3f}")
                logger.debug(f"      💰 ROI: {roi:.1f}% (mínimo: {self.min_roi}%)")

                if roi >= self.min_roi and p_real > p_prior:
                    logger.info(f"      ✅ APOSTA ENCONTRADA: {player} - ROI {roi:.1f}%")
                    good_bets.append({
                        "event_id": event_id,
                        "market_name": market_name,
                        "selection_line": selection_line,
                        "handicap": line,
                        "house_odds": odds,
                        "roi_average": roi,
                        "fair_odds": fair,
                        "actual_value": None,
                        "odds_type": "player",  # ✅ ADICIONAR
                        "map_number": 1,  # ✅ ADICIONAR
                    })
                else:
                    logger.debug(f"      ❌ ROI insuficiente ou probabilidade baixa")

        except Exception as e:
            logger.error(f"❌ Erro ao analisar players: {e}")

        logger.info(f"🎯 Total de apostas de players válidas: {len(good_bets)}")
        return good_bets

    def _analyze_total_markets(self, event_data: Dict, stake: float) -> List[Dict]:
        """Analisa mercados de Totals (Map 1 e Map 2) usando APENAS dados reais"""
        all_good_bets = []
        event_id = event_data["event_id"]

        logger.info(f"📈 Analisando totals para evento {event_id}")

        if not self.odds_db_path:
            logger.warning("⏭️ Caminho do banco de odds não configurado")
            return all_good_bets

        if not self.roi_analyzer:
            logger.warning("⏭️ ROIAnalyzer não disponível")
            return all_good_bets

        try:
            conn = sqlite3.connect(str(self.odds_db_path))
            
            query = """
                SELECT market_type, selection, line, odds, map_number, odds_type
                FROM current_odds
                WHERE event_id = ?
                AND market_type IN ('Map 1 - Totals', 'Map 2 - Totals')
                AND odds_type IN ('map_1', 'map_2')
            """
            
            logger.info(f"🔍 Buscando totals para evento {event_id}")
            cursor = conn.cursor()
            cursor.execute(query, (event_id,))
            rows = cursor.fetchall()
            conn.close()
            
            logger.info(f"📊 Encontradas {len(rows)} linhas de totals no banco")
            
            if not rows:
                logger.info("📭 Nenhuma linha de totals encontrada")
                return all_good_bets
            
            # Buscar informações dos times
            team1 = None
            team2 = None
            
            try:
                event_info = self.roi_analyzer.get_event_info(event_id)
                if event_info:
                    team1 = event_info.get("home_team", "Team A")
                    team2 = event_info.get("away_team", "Team B")
                    logger.info(f"🏠 Time da casa: {team1}")
                    logger.info(f"🚗 Time visitante: {team2}")
                else:
                    logger.warning("⚠️ Não foi possível obter informações dos times")
                    return all_good_bets
            except Exception as e:
                logger.error(f"❌ Erro ao buscar info dos times: {e}")
                return all_good_bets
            
            # Processar cada linha
            for row in rows:
                market_type, selection, line_str, odds, map_number, odds_type = row
                
                try:
                    handicap = float(line_str)
                except:
                    logger.debug(f"   ❌ Não foi possível extrair handicap de: {line_str}")
                    continue
                
                odds_value = float(odds)
                
                logger.debug(f"   Processando: {market_type} - {selection} {handicap} @ {odds_value}")
                
                # ✅ APENAS ROIAnalyzer com dados reais
                try:
                    roi_team1, roi_team2, roi_average, fair_odds_average = (
                        self.roi_analyzer.calculate_average_roi(
                            team1, team2, selection, handicap, odds_value
                        )
                    )
                    
                    logger.debug(f"      ROI: {roi_average:.1f}%")
                    
                    if roi_average > self.min_roi:
                        logger.info(f"      ✅ APOSTA TOTAL: {market_type} - {selection} {handicap} - ROI {roi_average:.1f}%")
                        bet_data = {
                            "event_id": event_id,
                            "market_name": market_type,
                            "selection_line": selection,
                            "handicap": handicap,
                            "house_odds": odds_value,
                            "roi_average": roi_average,
                            "fair_odds": fair_odds_average,
                            "actual_value": None,
                            "odds_type": odds_type,
                            "map_number": map_number if map_number else (1 if 'Map 1' in market_type else 2),
                        }
                        all_good_bets.append(bet_data)
                    else:
                        logger.debug(f"      ❌ ROI insuficiente: {roi_average:.1f}%")
                        
                except Exception as e:
                    logger.debug(f"      ❌ Erro ao calcular ROI: {e}")
                    continue
            
            # Ordenar por ROI
            all_good_bets.sort(key=lambda x: x["roi_average"], reverse=True)
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar totals: {e}", exc_info=True)

        logger.info(f"🎯 Total de apostas de totals válidas: {len(all_good_bets)}")
        return all_good_bets


class BasicStrategy(BettingStrategy):
    """Estratégia básica alternativa (para demonstração)"""
    
    def __init__(self, min_roi: float = 10.0):
        self.min_roi = min_roi
    
    def get_description(self) -> str:
        return f"Estratégia Básica (ROI mínimo: {self.min_roi}%)"
    
    def analyze(self, event_data: Dict, stake: float) -> List[Dict]:
        """Implementação básica - pode ser expandida depois"""
        return []


# Factory function
def get_strategy(strategy_name: str, **kwargs) -> BettingStrategy:
    strategies = {
        "statistical": StatisticalStrategy,
        "basic": BasicStrategy,
    }
    
    if strategy_name not in strategies:
        raise ValueError(f"Estratégia não encontrada: {strategy_name}")
    
    return strategies[strategy_name](**kwargs)