import sqlite3

from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import DictCursor

from dataclasses import dataclass

MANY_TO_MANY_TABLES = ['genre_film_work', 'person_film_work']
TABLE_NAME_METHODS = {
    'genre': 'genre_init',
    'genre_film_work': 'genre_film_work_init',
    'person_film_work': 'person_film_work_init',
    'person': 'person_init',
    'film_work': 'film_work_init',
}
SQL_SELECT = """SELECT {} FROM {};"""
TIMESTAMP_WITH_TIMEZONE = datetime.now(timezone.utc)


@dataclass()
class Table:
    """Инициализатор таблиц"""
    name: str

    def __transfer_data_to_psql(
            self,
            sqlite_extractor,
            pg_connect,
            dict_fields,
            ignore_fields=None,
            datetime_fields=None
    ):
        """Перенос данных между таблицами"""
        if ignore_fields is None:
            ignore_fields = []
        headers = sqlite_extractor.get_headers(self.name, ignore_fields)
        data = sqlite_extractor.get_all_data(self.name, headers)
        pg_connect.insert_data(headers, dict_fields, data, self.name, datetime_fields)

    def genre_init(self, cursor, pg_connect):
        mapping_fields = {
            'id': 'id',
            'name': 'name',
            'description': 'description',
            'created_at': 'created',
            'updated_at': 'modified'
        }
        datetime_fields = ['created_at', 'updated_at']
        self.__transfer_data_to_psql(cursor, pg_connect, mapping_fields, datetime_fields=datetime_fields)

    def genre_film_work_init(self, cursor, pg_connect):
        mapping_fields = {
            'id': 'id',
            'film_work_id': 'film_work_id',
            'genre_id': 'genre_id',
            'created_at': 'created_at',
        }
        datetime_fields = ['created_at']
        self.__transfer_data_to_psql(cursor, pg_connect, mapping_fields, ignore_fields=None, datetime_fields=datetime_fields)

    def person_film_work_init(self, cursor, pg_connect):
        mapping_fields = {
            'id': 'id',
            'role': 'role',
            'film_work_id': 'film_work_id',
            'person_id': 'person_id',
            'created_at': 'created_at',
        }
        datetime_fields = ['created_at']
        self.__transfer_data_to_psql(cursor, pg_connect, mapping_fields, datetime_fields=datetime_fields)

    def person_init(self, cursor, pg_connect):
        mapping_fields = {
            'id': 'id',
            'full_name': 'full_name',
            'created_at': 'created',
            'updated_at': 'modified'
        }
        datetime_fields = ['created_at', 'updated_at']
        self.__transfer_data_to_psql(cursor, pg_connect, mapping_fields, datetime_fields=datetime_fields)

    def film_work_init(self, cursor, pg_connect):
        mapping_fields = {
            'id': 'id',
            'title': 'title',
            'description': 'description',
            'creation_date': 'creation_date',
            'rating': 'rating',
            'type': 'type',
            'created_at': 'created',
            'updated_at': 'modified'
        }
        datetime_fields = ['created_at', 'updated_at']
        ignore_fields = ['file_path']
        self.__transfer_data_to_psql(cursor, pg_connect, mapping_fields, ignore_fields=ignore_fields, datetime_fields=datetime_fields)


@dataclass
class PostgresSaver:
    data: dict
    conn_pg: sqlite3.Connection = None

    def __post_init__(self):
        self.conn_pg = psycopg2.connect(**self.data, cursor_factory=DictCursor)

    def close_connect(self):
        self.conn_pg.close()

    def rollback_connect(self):
        self.conn_pg.rollback()

    def __del__(self):
        self.close_connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connect()

    def insert_data(self, headers, dict_fields, data, table_name, datetime_fields=None):
        """Запись данных в Postgres"""
        if datetime_fields is None:
            datetime_fields = []
        values_s = '(' + ', '.join(['%s'] * len(headers)) + ')'
        headers_s = '(' + ', '.join([dict_fields[i] for i in headers]) + ')'
        with self.conn_pg.cursor() as pg_cursor:
            args = []
            for item in data:
                item = list(item)
                for datetime_field in datetime_fields:
                    index_datetime_field = headers.index(datetime_field)
                    item[index_datetime_field] = TIMESTAMP_WITH_TIMEZONE
                args.append(pg_cursor.mogrify(values_s, item).decode())
            args = ','.join(args)
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

    def __post_init__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.close()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_list_table(self):
        """Получить список всех табиц"""
        self.cursor.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
        return self.cursor.fetchall()

    def get_headers(self, table_name, ignore_fields):
        """Получить заголовки табоицы"""
        self.cursor.execute("""SELECT * FROM {} limit 1""".format(table_name))
        headers = [i[0] for i in self.cursor.description if i[0] not in ignore_fields]
        return headers

    def get_all_data(self, table_name, headers):
        """Получить все записи из таблицы"""
        self.cursor.execute(SQL_SELECT.format(', '.join(headers), table_name))
        data = self.cursor.fetchall()
        return data


def load_from_sqlite(sqlite_extractor, postgres_saver):
    """Основной метод загрузки данных из SQLite в Postgres"""
    list_table = sqlite_extractor.get_list_table()
    last_table = []
    for table in list_table:
        if table[0] in TABLE_NAME_METHODS:
            if table[0] in MANY_TO_MANY_TABLES:
                last_table.append(table[0])
                continue
            getattr(
                Table(table[0]),
                TABLE_NAME_METHODS[table[0]]
            )(sqlite_extractor, postgres_saver)
    for table in last_table:
        getattr(
            Table(table),
            TABLE_NAME_METHODS[table]
        )(sqlite_extractor, postgres_saver)


if __name__ == '__main__':
    dsl = {
        'dbname': 'movies_database',
        'user': 'app',
        'password': '123qwe',
        'host': '127.0.0.1',
        'port': 5432
    }
    sqlite3_path = 'db.sqlite'
    with PostgresSaver(dsl) as pg_conn, SQLiteExtractor(sqlite3_path) as sqlite_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
