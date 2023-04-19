import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from dataclasses import dataclass

TABLE_NAME_METHODS = {
    'genre': 'genre_insert',
    'genre_film_work': 'genre_film_work_insert',
    'person_film_work': 'person_film_work_insert',
    'person': 'person_insert',
    'film_work': 'film_work_insert',
}
SQL_SELECT_HEADER = 'select * from {} limit 1'
SQL_SELECT = 'select {} from {};'
SQL_INSERT = """
                INSERT INTO content.{table_name} {headers}
                VALUES {args}
                on conflict (id) do nothing
                {on_conflict};
                """
SQL_CONFLICT_OTHER = """ on conflict ({}) do nothing"""


@dataclass()
class Table:
    name: str

    def __transfer_data_to_psql(
            self,
            sqlite_extractor,
            pg_connect,
            dict_fields,
            uniq_fields=None,
            ignore_fields=None
    ):
        if ignore_fields is None:
            ignore_fields = []
        if uniq_fields is None:
            uniq_fields = []
        headers = sqlite_extractor.get_headers(self.name, ignore_fields)
        data = sqlite_extractor.get_all_data(self.name, headers)
        pg_connect.insert_data(headers, dict_fields, data, self.name, uniq_fields)

    def genre_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'name': 'name',
            'description': 'description',
            'created_at': 'created',
            'updated_at': 'modified'
        }
        self.__transfer_data_to_psql(cursor, pg_connect, dict_fields)

    def genre_film_work_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'film_work_id': 'film_work_id',
            'genre_id': 'genre_id',
            'created_at': 'created_at',
        }
        uniq_fields = [('film_work_id', 'genre_id'),]
        self.__transfer_data_to_psql(cursor, pg_connect, dict_fields, uniq_fields)

    def person_film_work_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'role': 'role',
            'film_work_id': 'film_work_id',
            'person_id': 'person_id',
            'created_at': 'created_at',
        }
        uniq_fields = [('film_work_id', 'person_id', 'role'),]
        self.__transfer_data_to_psql(cursor, pg_connect, dict_fields, uniq_fields)

    def person_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'full_name': 'full_name',
            'created_at': 'created',
            'updated_at': 'modified'
        }
        self.__transfer_data_to_psql(cursor, pg_connect, dict_fields)

    def film_work_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'title': 'title',
            'description': 'description',
            'creation_date': 'creation_date',
            'rating': 'rating',
            'type': 'type',
            'created_at': 'created',
            'updated_at': 'modified'
        }
        ignore_fields = ['file_path']
        self.__transfer_data_to_psql(cursor, pg_connect, dict_fields, ignore_fields)


@dataclass
class PostgresSaver:
    data: dict
    conn_pg: sqlite3.Connection = None

    def __post_init__(self):
        self.conn_pg = psycopg2.connect(**self.data, cursor_factory=DictCursor)

    def close_connect(self):
        self.conn_pg.close()

    def __del__(self):
        self.close_connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connect()

    def insert_data(self, headers, dict_fields, data, table_name, uniq_fields):
        values_s = '(' + ', '.join(['%s'] * len(headers)) + ')'
        headers = '(' + ', '.join([dict_fields[i] for i in headers]) + ')'
        sql_uniq_fields = ''
        for uniq_fields_once in uniq_fields:
            sql_uniq_fields += SQL_CONFLICT_OTHER.format(', '.join(uniq_fields_once))
        with self.conn_pg.cursor() as pg_cursor:
            args = ','.join(pg_cursor.mogrify(values_s, item).decode() for item in data)
            sql_text = SQL_INSERT.format(table_name=table_name, headers=headers, args=args, on_conflict=sql_uniq_fields)
            pg_cursor.execute(sql_text)

    def get_foreing_keys(self, table_name):
        with self.conn_pg.cursor() as cursor:
            sql_text = """select * from information_schema.table_constraint
             where table_name='{}' and constraint_type='FOREIGN KEY';"""\
                .format(table_name)
            cursor.execute(sql_text)
            f_keys = cursor.fetchall()
            return f_keys


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
        self.cursor.execute('''SELECT name FROM sqlite_master WHERE type='table';''')
        return self.cursor.fetchall()

    def get_headers(self, table_name, ignore_fields):
        self.cursor.execute(SQL_SELECT_HEADER.format(table_name))
        headers = [i[0] for i in self.cursor.description if i[0] not in ignore_fields]
        return headers

    def get_all_data(self, table_name, headers):
        self.cursor.execute(SQL_SELECT.format(', '.join(headers), table_name))
        data = self.cursor.fetchall()
        return data


def load_from_sqlite(sqlite_extractor, postgres_saver):
    """Основной метод загрузки данных из SQLite в Postgres"""
    list_table = sqlite_extractor.get_list_table()
    last_table = []
    for table in list_table:
        if table[0] in TABLE_NAME_METHODS:
            f_keys = postgres_saver.get_foreing_keys(table[0])
            if f_keys:
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
