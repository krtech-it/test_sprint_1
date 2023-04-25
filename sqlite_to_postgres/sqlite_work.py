from dataclasses import dataclass
import sqlite3

from settings import SQL_SELECT


@dataclass
class SQLiteExtractor:
    db_path: str
    conn: sqlite3.Connection = None
    cursor: sqlite3.Cursor = None

    def __post_init__(self) -> None:
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def close(self) -> None:
        self.conn.close()

    def __del__(self) -> None:
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def get_list_table(self) -> list:
        """Получить список всех табиц"""
        self.cursor.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
        return self.cursor.fetchall()

    def get_headers(self, table_name: str) -> set:
        """Получить заголовки табоицы"""
        self.cursor.execute("""SELECT * FROM {} limit 1""".format(table_name))
        headers = [i[0] for i in self.cursor.description]
        return set(headers)

    def get_all_data(self, table_name: str, headers: set) -> sqlite3.Cursor:
        """Получить все записи из таблицы"""
        self.cursor.execute(SQL_SELECT.format(', '.join(headers), table_name))
        self.cursor.fetchmany()
        # data = self.cursor.fetchall()
        return self.cursor
