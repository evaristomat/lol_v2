#!/usr/bin/env python3
"""
Sistema de Verificação de Resultados de Apostas LoL
Arquivo principal que orquestra todo o processamento
"""

import sys
import time
from pathlib import Path

# Adiciona src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from colorama import Fore, Style, init
from src.get_results.cache import cache_manager
from src.get_results.config import Config, setup_logger
from src.get_results.database import connection_pool
from src.get_results.models import BetResult, BetStatus
from src.get_results.processors import PlayerProcessor, TotalsProcessor
from src.get_results.repositories import BetsRepository

init(autoreset=True)
logger = setup_logger()


class BetVerificationSystem:
    """Sistema principal de verificação de apostas"""

    def __init__(self):
        self.totals_processor = TotalsProcessor()
        self.player_processor = PlayerProcessor()
        self.stats = {BetStatus.WON: 0, BetStatus.LOST: 0, BetStatus.NOT_FOUND: 0}

    def is_player_bet(self, market_type: str) -> bool:
        """Verifica se é aposta de jogador"""
        return "Player" in str(market_type)

    def process_bet(self, bet) -> BetResult:
        """Processa uma aposta individual"""
        try:
            if self.is_player_bet(bet["market_type"]):
                return self.player_processor.process(bet)
            else:
                return self.totals_processor.process(bet)
        except Exception as e:
            logger.error("Erro ao processar aposta %d: %s", bet["id"], str(e))
            return None

    def process_batch_parallel(self, bets):
        """Processa lote de apostas em paralelo"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = []

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            # Submeter todas as tarefas
            future_to_bet = {
                executor.submit(self.process_bet, bet): idx
                for idx, bet in bets.iterrows()
            }

            # Processar resultados conforme completam
            for future in as_completed(future_to_bet):
                idx = future_to_bet[future]
                try:
                    result = future.result(timeout=30)
                    if result:
                        results.append(result)
                        self.stats[result.bet_status] += 1

                        # Log do resultado
                        if result.won:
                            status_str = f"{Fore.GREEN}WON{Style.RESET_ALL}"
                        else:
                            status_str = f"{Fore.RED}LOST{Style.RESET_ALL}"

                        logger.info(
                            "  [%d/%d] Aposta %d: %s (Valor: %.1f, Tempo: %.2fs)",
                            idx + 1,
                            len(bets),
                            result.bet_id,
                            status_str,
                            result.actual_value,
                            result.processing_time,
                        )
                    else:
                        self.stats[BetStatus.NOT_FOUND] += 1
                        logger.info(
                            "  [%d/%d] Aposta %d: %sNÃO ENCONTRADA%s",
                            idx + 1,
                            len(bets),
                            bets.iloc[idx]["id"],
                            Fore.YELLOW,
                            Style.RESET_ALL,
                        )

                except Exception as e:
                    logger.error("Erro no processamento paralelo: %s", str(e))
                    self.stats[BetStatus.NOT_FOUND] += 1

        return results

    def analyze_pending_bets(self, bets):
        """Analisa e exibe detalhes das apostas pendentes"""
        from datetime import datetime

        import pandas as pd

        logger.info(
            "\n%sANÁLISE DETALHADA DAS APOSTAS PENDENTES%s", Fore.CYAN, Style.RESET_ALL
        )

        analyses = BetsRepository.analyze_pending_bets(bets)

        # Contar por categoria
        categories = {
            "future": {"count": 0, "bets": []},
            "recent": {"count": 0, "bets": []},
            "recent_week": {"count": 0, "bets": []},
            "old": {"count": 0, "bets": []},
            "old_no_info": {"count": 0, "bets": []},
            "recent_no_info": {"count": 0, "bets": []},
        }

        for analysis in analyses:
            categories[analysis.status]["count"] += 1
            categories[analysis.status]["bets"].append(analysis)

        # Exibir resumo
        self._display_analysis_summary(categories, len(analyses))

        return categories, analyses

    def _display_analysis_summary(self, categories, total_bets):
        """Exibe resumo da análise"""
        from datetime import datetime

        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}📊 DISTRIBUIÇÃO DAS APOSTAS PENDENTES{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

        # Jogos futuros
        future_count = categories["future"]["count"]
        print(
            f"\n{Fore.GREEN}🎯 JOGOS FUTUROS: {future_count} apostas ({future_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Apostas em jogos que ainda vão acontecer)")

        # Jogos recentes
        recent_count = categories["recent"]["count"]
        print(
            f"\n{Fore.BLUE}🕐 JOGOS RECENTES (≤1 dia): {recent_count} apostas ({recent_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Jogos que aconteceram nas últimas 24h)")

        # Jogos da semana
        recent_week_count = categories["recent_week"]["count"]
        print(
            f"\n{Fore.YELLOW}📅 JOGOS DA SEMANA (2-7 dias): {recent_week_count} apostas ({recent_week_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Jogos que aconteceram há 2-7 dias)")

        # Jogos antigos
        old_count = categories["old"]["count"]
        print(
            f"\n{Fore.RED}🚨 JOGOS ANTIGOS (>7 dias): {old_count} apostas ({old_count/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   (Jogos que aconteceram há mais de 7 dias - PRECISAM DE ATENÇÃO!)")

        # Sem informação
        old_no_info = categories["old_no_info"]["count"]
        recent_no_info = categories["recent_no_info"]["count"]
        print(
            f"\n{Fore.MAGENTA}❓ SEM INFORMAÇÃO DE TIMES: {old_no_info + recent_no_info} apostas ({(old_no_info + recent_no_info)/total_bets*100:.1f}%){Style.RESET_ALL}"
        )
        print(f"   - Antigas (>7 dias): {old_no_info}")
        print(f"   - Recentes (≤7 dias): {recent_no_info}")

        # Detalhes problemáticos
        if categories["old"]["count"] > 0:
            print(
                f"\n{Fore.RED}{'🚨 APOSTAS PROBLEMÁTICAS (JOGOS ANTIGOS) 🚨':^60}{Style.RESET_ALL}"
            )
            for analysis in categories["old"]["bets"][:10]:
                print(
                    f"   Bet {analysis.bet_id}: {analysis.event_id} - {analysis.market_type}"
                )
                print(
                    f"        {analysis.selection} | Jogo há {analysis.days_since_match} dias"
                )

            if len(categories["old"]["bets"]) > 10:
                print(f"   ... e mais {len(categories['old']['bets']) - 10} apostas")

    def _display_summary(
        self, total_processed: int, elapsed_time: float, categories=None
    ):
        """Exibe resumo final da execução"""
        print(f"{Fore.CYAN}{Style.BRIGHT}\n{'=' * 60}")
        print(f"{Fore.CYAN}{Style.BRIGHT}📊 RESUMO FINAL")
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")

        total_won_lost = sum(self.stats[s] for s in [BetStatus.WON, BetStatus.LOST])

        print(f"{Fore.WHITE}{Style.BRIGHT}\n⏱️  Tempo total: {elapsed_time:.2f}s")
        if elapsed_time > 0 and total_processed > 0:
            print(
                f"{Fore.WHITE}{Style.BRIGHT}📈 Taxa: {total_processed / elapsed_time:.1f} apostas/s"
            )

        if categories:
            total_bets = sum(cat["count"] for cat in categories.values())
            print(
                f"{Fore.WHITE}{Style.BRIGHT}\n📋 Total de apostas pendentes: {total_bets}"
            )
            print(f"{Fore.GREEN}  ✓ Jogos futuros: {categories['future']['count']}")
            print(
                f"{Fore.BLUE}  ✓ Jogos recentes (≤1 dia): {categories['recent']['count']}"
            )
            print(
                f"{Fore.YELLOW}  ✓ Jogos da semana (2-7 dias): {categories['recent_week']['count']}"
            )
            print(
                f"{Fore.RED}  🚨 Jogos antigos (>7 dias): {categories['old']['count']}"
            )
            print(
                f"{Fore.MAGENTA}  ❓ Sem info times: {categories['old_no_info']['count'] + categories['recent_no_info']['count']}"
            )

        print(
            f"{Fore.WHITE}{Style.BRIGHT}\n🔍 Apostas processadas (jogos passados): {total_processed}"
        )
        print(f"{Fore.GREEN}{Style.BRIGHT}  ✓ Apostas atualizadas: {total_won_lost}")
        print(f"{Fore.GREEN}    - Ganhas: {self.stats[BetStatus.WON]}")
        print(f"{Fore.RED}    - Perdidas: {self.stats[BetStatus.LOST]}")
        print(f"{Fore.YELLOW}  ⚠ Sem resultado: {self.stats[BetStatus.NOT_FOUND]}")

        # Taxa de sucesso
        if total_won_lost > 0:
            win_rate = (self.stats[BetStatus.WON] / total_won_lost) * 100
            print(f"{Fore.CYAN}\n🎯 Taxa de acerto: {win_rate:.1f}%")

        # Recomendações
        if categories and categories["old"]["count"] > 0:
            print(
                f"{Fore.RED}\n🚨 RECOMENDAÇÃO: {categories['old']['count']} apostas com jogos antigos precisam de atenção!"
            )
            print(
                f"   Verifique se os dados desses jogos estão disponíveis no CSV/Histórico{Style.RESET_ALL}"
            )

        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")

    def run(self):
        """Executa o sistema de verificação"""
        import pandas as pd
        from src.get_results.config import TEAM_NAME_MAPPINGS

        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}")
        print(f"{Fore.CYAN}{Style.BRIGHT}🚀 SISTEMA DE VERIFICAÇÃO DE RESULTADOS v2.0")
        print(
            f"{Fore.CYAN}{Style.BRIGHT}📝 COM MAPEAMENTO DE TIMES E TOLERÂNCIA DE DATA"
        )
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")

        # Exibir informações das melhorias
        print(f"{Fore.GREEN}✅ Melhorias implementadas:{Style.RESET_ALL}")
        print(f"   • Mapeamento de {len(TEAM_NAME_MAPPINGS)} nomes de times")
        print(f"   • Tolerância de +/- {Config.DATE_TOLERANCE_DAYS} dia(s) para datas")
        print(f"   • Busca flexível por nomes similares")
        print()

        start_time = time.time()

        try:
            # 1. Carregar apostas pendentes
            logger.info("Iniciando verificação de resultados...")
            bets = BetsRepository.get_pending_bets()

            if len(bets) == 0:
                logger.info("Nenhuma aposta pendente")
                return

            # 2. Análise detalhada
            categories, all_analyses = self.analyze_pending_bets(bets)

            # 3. Criar dicionário para acesso rápido
            analysis_dict = {analysis.bet_id: analysis for analysis in all_analyses}

            # 4. Separar apostas para processamento
            bets_to_process = []
            for _, bet in bets.iterrows():
                bet_id = bet["id"]
                analysis = analysis_dict.get(bet_id)
                if analysis and analysis.status in [
                    "recent",
                    "recent_week",
                    "old",
                    "old_no_info",
                ]:
                    bets_to_process.append(bet)

            bets_to_process_df = (
                pd.DataFrame(bets_to_process) if bets_to_process else pd.DataFrame()
            )

            logger.info(
                "\nProcessando %d apostas (jogos que já ocorreram) de %d totais",
                len(bets_to_process_df),
                len(bets),
            )

            if len(bets_to_process_df) == 0:
                logger.info("Nenhuma aposta de jogos passados para processar")
                self._display_summary(0, time.time() - start_time, categories)
                return

            # 5. Pré-carregar cache
            logger.info("Pré-carregando dados em cache...")
            cache_manager.load_csv()

            # 6. Processar em lotes
            logger.info(
                "Processando %d apostas em paralelo...", len(bets_to_process_df)
            )
            all_results = []

            for i in range(0, len(bets_to_process_df), Config.BATCH_SIZE):
                batch = bets_to_process_df.iloc[i : i + Config.BATCH_SIZE]
                logger.info(
                    "Processando lote %d (%d apostas)...",
                    i // Config.BATCH_SIZE + 1,
                    len(batch),
                )

                batch_results = self.process_batch_parallel(batch)
                all_results.extend(batch_results)

                # Atualizar banco em batch
                if batch_results:
                    BetsRepository.batch_update_results(batch_results)
                    logger.info(
                        "Lote %d atualizado: %d apostas processadas",
                        i // Config.BATCH_SIZE + 1,
                        len(batch_results),
                    )
                else:
                    logger.warning(
                        "Lote %d: nenhuma aposta processada", i // Config.BATCH_SIZE + 1
                    )

            # 7. Exibir resumo final
            self._display_summary(
                len(bets_to_process_df), time.time() - start_time, categories
            )

        except Exception as e:
            logger.error("Erro fatal no sistema: %s", str(e))
            raise
        finally:
            # Limpeza
            connection_pool.close_all()
            cache_manager.clear()


def main():
    """Função principal"""
    try:
        system = BetVerificationSystem()
        system.run()
    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário")
    except Exception as e:
        logger.error("Erro fatal: %s", str(e))
        raise


if __name__ == "__main__":
    main()
