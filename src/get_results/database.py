"""
Gerenciamento de conexões com banco de dados
"""

import sqlite3
import threading
from contextlib import contextmanager
from typing import Dict

from .config import Config, setup_logger

logger = setup_logger()


class ConnectionPool:
    """Pool de conexões para reutilização"""

    def __init__(self):
        self._connections: Dict[str, sqlite3.Connection] = {}
        self._lock = threading.RLock()

    @contextmanager
    def get_connection(self, db_path: str):
        """Context manager para obter conexão"""
        with self._lock:
            if db_path not in self._connections:
                try:
                    conn = sqlite3.connect(
                        db_path,
                        timeout=Config.CONNECTION_TIMEOUT,
                        check_same_thread=False,
                    )
                    # Otimizações SQLite
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA journal_mode=WAL")
                    cursor.execute("PRAGMA synchronous=NORMAL")
                    cursor.execute("PRAGMA cache_size=10000")
                    cursor.execute("PRAGMA temp_store=MEMORY")
                    cursor.close()

                    self._connections[db_path] = conn
                except Exception as e:
                    logger.error("Erro ao conectar com %s: %s", db_path, str(e))
                    raise

            yield self._connections[db_path]

    def close_all(self):
        """Fecha todas as conexões"""
        with self._lock:
            for db_path, conn in self._connections.items():
                try:
                    conn.close()
                except Exception as e:
                    logger.error("Erro ao fechar conexão %s: %s", db_path, str(e))
            self._connections.clear()


# Instância global do pool
connection_pool = ConnectionPool()
