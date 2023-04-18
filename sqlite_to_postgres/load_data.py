import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import uuid
import datetime
from dataclasses import dataclass, field

TABLE_NAME_METHODS = {
    'genre': 'genre_insert',
    'genre_film_work': 'genre_film_work_insert',
    'person_film_work': 'person_film_work_insert',
    'person': 'person_insert',
    'film_work': 'film_work_insert',
}


@dataclass
class FilmWork:
    title: str
    description: str
    creation_date: str
    rating: float = field(default=0.0)
    id: uuid.uuid4 = field(default_factory=uuid.uuid4)
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    modified_at: datetime.datetime = field(default_factory=datetime.datetime.now)


@dataclass()
class Table:
    name: str

    SQL_SELECT = 'select {} from {};'
    SQL_SELECT_HEADER = 'select * from {} limit 1'
    SQL_INSERT = """
                INSERT INTO content.{table_name} {headers}
                VALUES {args}
                on conflict (id) do nothing;
                """

    def __transfer_data_to_psql(self, cursor, pg_connect, dict_fields, ignore_fields=None):
        if ignore_fields is None:
            ignore_fields = []
        cursor.execute(self.SQL_SELECT_HEADER.format(self.name))
        headers = [i[0] for i in cursor.description if i[0] not in ignore_fields]
        cursor.execute(self.SQL_SELECT.format(', '.join(headers), self.name))
        data = cursor.fetchall()
        values_s = '(' + ', '.join(['%s'] * len(headers)) + ')'
        headers = '(' + ', '.join([dict_fields[i] for i in headers]) + ')'
        with pg_conn.cursor() as pg_cursor:
            args = ','.join(pg_cursor.mogrify(values_s, item).decode() for item in data)
            sql_text = self.SQL_INSERT.format(table_name=self.name, headers=headers, args=args)
            pg_cursor.execute(sql_text)

    def genre_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'name': 'name',
            'description': 'description',
            'created_at': 'created',
            'updated_at': 'modified'
        }
        self.__transfer_data_to_psql(cursor, pg_conn, dict_fields)

    def genre_film_work_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'film_work_id': 'film_work_id',
            'genre_id': 'genre_id',
            'created_at': 'created_at',
        }
        self.__transfer_data_to_psql(cursor, pg_conn, dict_fields)

    def person_film_work_insert(self, cursor, pg_connect):
        dict_fields = {
            'id': 'id',
            'role': 'role',
            'film_work_id': 'film_work_id',
            'person_id': 'person_id',
            'created_at': 'created_at',
        }
        self.__transfer_data_to_psql(cursor, pg_conn, dict_fields)

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


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    # postgres_saver = PostgresSaver(pg_conn)
    # sqlite_extractor = SQLiteExtractor(connection)
    # data = sqlite_extractor.extract_movies()
    # postgres_saver.save_all_data(data)




    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    last_table = []
    for table in tables:
        if table[0] in TABLE_NAME_METHODS:
            with pg_conn.cursor() as pg_cursor:
                sql_text = '''select * from information_schema.table_constraints where table_name='{}' and constraint_type='FOREIGN KEY';'''.format(table[0])
                pg_cursor.execute(sql_text)
                f_keys = pg_cursor.fetchall()
                if f_keys:
                    last_table.append(table[0])
                    continue
            getattr(
                Table(table[0]),
                TABLE_NAME_METHODS[table[0]]
            )(cursor, pg_conn)
    for table in last_table:
        getattr(
            Table(table),
            TABLE_NAME_METHODS[table]
        )(cursor, pg_conn)


if __name__ == '__main__':
    dsl = {
        'dbname': 'movies_database',
        'user': 'app',
        'password': '123qwe',
        'host': '127.0.0.1',
        'port': 5432
    }
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
