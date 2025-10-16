import sqlite3
from contextlib import contextmanager
from pathlib import Path


class BaseDatabase:
    """Classe base para gerenciamento de bancos SQLite com conexões reutilizáveis"""

    def __init__(self, db_name: str):
        data_dir = Path(__file__).parent.parent.parent.parent / "data"
        data_dir.mkdir(exist_ok=True)
        self.db_path = str(data_dir / db_name)

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
