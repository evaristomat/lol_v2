# scripts/migrate_all_odds.py
import sqlite3
from datetime import datetime
from pathlib import Path


class AllOddsMigrator:
    def __init__(self):
        self.old_db = "data/lol_odds_old.db"
        self.new_db = "data/lol_odds.db"
        self.stats = {
            "odds_migrated": 0,
            "odds_skipped": 0,
            "errors": [],
        }

    def validate_databases(self):
        """Valida se os bancos existem"""
        if not Path(self.old_db).exists():
            raise FileNotFoundError(f"❌ Banco antigo não encontrado: {self.old_db}")

        if not Path(self.new_db).exists():
            raise FileNotFoundError(f"❌ Banco novo não encontrado: {self.new_db}")

        print("✅ Ambos os bancos encontrados")

    def migrate_all_odds(self, batch_size=1000):
        """Migra TODAS as odds do banco antigo para o novo"""
        print("\n" + "=" * 60)
        print("📊 MIGRANDO TODAS AS ODDS DO BANCO ANTIGO")
        print("=" * 60)

        old_conn = sqlite3.connect(self.old_db)
        new_conn = sqlite3.connect(self.new_db)

        old_cursor = old_conn.cursor()
        new_cursor = new_conn.cursor()

        # Contar total de odds
        old_cursor.execute("SELECT COUNT(*) FROM current_odds")
        total_odds = old_cursor.fetchone()[0]

        print(f"📊 Total de odds no banco antigo: {total_odds:,}")
        print(f"⏳ Processando em batches de {batch_size}...")

        offset = 0
        processed = 0

        while True:
            # Buscar batch de odds do banco antigo
            old_cursor.execute(
                """
                SELECT event_id, odds_type, market_name, selection_name, 
                       odds_value, handicap, updated_at, raw_data
                FROM current_odds
                LIMIT ? OFFSET ?
            """,
                (batch_size, offset),
            )

            batch = old_cursor.fetchall()

            if not batch:
                break

            for row in batch:
                try:
                    (
                        event_id,
                        odds_type,
                        market_name,
                        selection_name,
                        odds_value,
                        handicap,
                        updated_at,
                        raw_data,
                    ) = row

                    processed += 1

                    # Verificar se a odd já existe no novo banco
                    new_cursor.execute(
                        """
                        SELECT id FROM current_odds 
                        WHERE event_id = ? AND odds_type = ? 
                        AND market_type = ? AND selection = ? AND line = ?
                    """,
                        (
                            event_id,
                            odds_type,
                            market_name,
                            selection_name,
                            handicap or "",
                        ),
                    )

                    if new_cursor.fetchone():
                        self.stats["odds_skipped"] += 1
                        continue

                    # Extrair map_number
                    map_number = None
                    if market_name and "Map 1" in market_name:
                        map_number = 1
                    elif market_name and "Map 2" in market_name:
                        map_number = 2
                    elif market_name and "Map 3" in market_name:
                        map_number = 3

                    # Inserir odd no novo banco (mesmo que o evento não exista na tabela events)
                    new_cursor.execute(
                        """
                        INSERT INTO current_odds (
                            event_id, odds_type, market_type, selection, odds, 
                            line, map_number, updated_at, raw_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            event_id,
                            odds_type,
                            market_name,  # → market_type
                            selection_name,  # → selection
                            odds_value,  # → odds
                            handicap or "",  # → line
                            map_number,
                            updated_at,
                            raw_data,
                        ),
                    )

                    self.stats["odds_migrated"] += 1

                except Exception as e:
                    self.stats["errors"].append(f"Odd event {event_id}: {str(e)[:100]}")

            # Commit do batch
            new_conn.commit()

            offset += batch_size
            progress = min(offset, total_odds)
            percentage = (progress / total_odds) * 100

            if offset % 5000 == 0:
                print(
                    f"   📊 Progresso: {progress:,}/{total_odds:,} ({percentage:.1f}%) - Migradas: {self.stats['odds_migrated']:,}"
                )

        old_conn.close()
        new_conn.close()

        print(f"✅ Odds migradas: {self.stats['odds_migrated']:,}")
        print(f"⏭️ Odds puladas (já existiam): {self.stats['odds_skipped']:,}")

    def show_summary(self):
        """Mostra resumo da migração"""
        print("\n" + "=" * 60)
        print("📊 RESUMO DA MIGRAÇÃO COMPLETA")
        print("=" * 60)

        new_conn = sqlite3.connect(self.new_db)
        new_cursor = new_conn.cursor()

        new_cursor.execute("SELECT COUNT(*) FROM current_odds")
        total_odds = new_cursor.fetchone()[0]

        new_conn.close()

        print(f"\n📈 BANCO NOVO APÓS MIGRAÇÃO:")
        print(f"   • Total de odds: {total_odds:,}")

        print(f"\n✅ ODDS MIGRADAS: {self.stats['odds_migrated']:,}")

        if self.stats["errors"]:
            print(f"\n❌ ERROS ({len(self.stats['errors'])}):")
            for i, error in enumerate(self.stats["errors"][:5], 1):
                print(f"   {i}. {error}")
            if len(self.stats["errors"]) > 5:
                print(f"   ... e mais {len(self.stats['errors']) - 5} erros")
        else:
            print(f"\n✅ Nenhum erro encontrado!")

    def run(self):
        """Executa a migração completa"""
        print("🚀 INICIANDO MIGRAÇÃO COMPLETA DE ODDS")
        print("=" * 60)

        try:
            self.validate_databases()

            # Mostrar situação atual
            old_conn = sqlite3.connect(self.old_db)
            new_conn = sqlite3.connect(self.new_db)

            old_cursor = old_conn.cursor()
            new_cursor = new_conn.cursor()

            old_cursor.execute("SELECT COUNT(*) FROM current_odds")
            old_total_odds = old_cursor.fetchone()[0]

            new_cursor.execute("SELECT COUNT(*) FROM current_odds")
            new_total_odds = new_cursor.fetchone()[0]

            old_conn.close()
            new_conn.close()

            print(f"\n📊 SITUAÇÃO ATUAL:")
            print(f"   Banco antigo: {old_total_odds:,} odds")
            print(f"   Banco novo: {new_total_odds:,} odds")

            print(f"\n🎯 ESTA MIGRAÇÃO IRÁ:")
            print(f"   • Migrar TODAS as odds do banco antigo")
            print(f"   • Ignorar apenas odds duplicadas")
            print(
                f"   • As odds serão migradas mesmo que os eventos não existam na tabela events"
            )

            confirm = input("\n🤔 Deseja continuar? (sim/não): ")

            if confirm.lower() not in ["sim", "s", "yes", "y"]:
                print("❌ Migração cancelada")
                return

            # Executar migração
            self.migrate_all_odds()

            # Mostrar resumo
            self.show_summary()

            print("\n✅ MIGRAÇÃO COMPLETA CONCLUÍDA!")

        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    migrator = AllOddsMigrator()
    migrator.run()
