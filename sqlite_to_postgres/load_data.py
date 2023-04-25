import sqlite3
import os
from dotenv import load_dotenv

from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import DictCursor

from dataclasses import dataclass, fields

load_dotenv()

dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT')
    }
sqlite3_path = os.environ.get('SQLITE_PATH')
MANY_TO_MANY_TABLES = ['genre_film_work', 'person_film_work']
SQL_SELECT = """SELECT {} FROM {};"""
TIMESTAMP_WITH_TIMEZONE = datetime.now(timezone.utc)


@dataclass
class PostgresSaver:
    data: dict
    conn_pg: psycopg2.extensions.connection = None

    def __post_init__(self) -> None:
        self.conn_pg = psycopg2.connect(**self.data, cursor_factory=DictCursor)

    def close_connect(self) -> None:
        self.conn_pg.close()

    def rollback_connect(self) -> None:
        self.conn_pg.rollback()

    def __del__(self) -> None:
        self.close_connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_connect()

    def insert_data(self,
                    headers: set,
                    data: list,
                    table_name: str) -> None:
        """Запись данных в Postgres"""
        values_s = '(' + ', '.join(['%s'] * len(headers)) + ')'
        headers_s = '(' + ', '.join([i for i in headers]) + ')'
        with self.conn_pg.cursor() as pg_cursor:
            args = ','.join([pg_cursor.mogrify(values_s, item).decode() for item in data])
            sql_text = """
                INSERT INTO content.{table_name} {headers}
                VALUES {args}
                on conflict (id) do nothing;
                """.format(table_name=table_name, headers=headers_s, args=args,)
            pg_cursor.execute(sql_text)
        self.conn_pg.commit()


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

    def get_all_data(self, table_name: str, headers: set) -> list:
        """Получить все записи из таблицы"""
        self.cursor.execute(SQL_SELECT.format(', '.join(headers), table_name))
        data = self.cursor.fetchall()
        return data


class TableWork:
    def sorted_values(self, headers: list) -> tuple:
        return tuple(self.__dict__.get(colm) for colm in headers)

    @classmethod
    def create_headers_list(cls,
                            sqlite_extractor: SQLiteExtractor,
                            table: str) -> tuple:
        headers = sqlite_extractor.get_headers(table)
        table_class_headers = set(map(lambda x: x.name, fields(cls)))
        headers = headers.intersection(table_class_headers)
        return headers

    @classmethod
    def create_data_for_insert(cls,
                               headers: set,
                               data: list) -> dict:
        for n, table_data in enumerate(data):
            table_data = dict(zip(tuple(headers), table_data))
            table_class = cls(**table_data)
            data[n] = (table_class.sorted_values(headers))
        return data


@dataclass
class FilmWork(TableWork):
    id: str
    title: str
    description: str
    creation_date: str
    rating: str
    type: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE
    updated_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class Genre(TableWork):
    id: str
    name: str
    description: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE
    updated_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class Person(TableWork):
    id: str
    full_name: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE
    updated_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class GenreFilmWork(TableWork):
    id: str
    film_work_id: str
    genre_id: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class PersonFilmWork(TableWork):
    id: str
    film_work_id: str
    person_id: str
    role: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE


def load_from_sqlite(sqlite_extractor, postgres_saver):
    """Основной метод загрузки данных из SQLite в Postgres"""
    list_table = sqlite_extractor.get_list_table()
    last_table = []
    for table in list_table:
        table = table[0]
        if table in TABLE_NAME_CLASSES:
            if table in MANY_TO_MANY_TABLES:
                last_table.append(table)
                continue
            headers = TABLE_NAME_CLASSES[table].create_headers_list(sqlite_extractor, table)
            data = sqlite_extractor.get_all_data(table, headers)
            data = TABLE_NAME_CLASSES[table].create_data_for_insert(headers, data)
            postgres_saver.insert_data(headers=headers, data=data, table_name=table)

    for table in last_table:
        headers = TABLE_NAME_CLASSES[table].create_headers_list(sqlite_extractor, table)
        data = sqlite_extractor.get_all_data(table, headers)
        data = TABLE_NAME_CLASSES[table].create_data_for_insert(headers, data)
        postgres_saver.insert_data(headers=headers, data=data, table_name=table)


if __name__ == '__main__':
    TABLE_NAME_CLASSES = {
        'genre': Genre,
        'genre_film_work': GenreFilmWork,
        'person_film_work': PersonFilmWork,
        'person': Person,
        'film_work': FilmWork,
    }
    with PostgresSaver(dsl) as pg_conn, SQLiteExtractor(sqlite3_path) as sqlite_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
