import sqlite3
from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import uuid
import datetime
from dataclasses import dataclass, field

TABLE_NAME_METHODS = {
    'genre': 'genre_insert',
    'genre_film_work': 'genre_film_work_select',
    'person_film_work': 'person_film_work_select',
    'person': 'person_select',
    'film_work': 'film_work_select',
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
    sql_select: str = field(default='select * from {};')

    def genre_insert(self, cursor, pg_conn):
        cursor.execute(self.sql_select.format(self.name))
        data = cursor.fetchall()
        print(data[0])
        with pg_conn.cursor() as pg_cursor:
            args = ','.join(pg_cursor.mogrify("(%s, %s, %s, %s, %s)", item).decode() for item in data)
            pg_cursor.execute(f"""
                INSERT INTO content.{self.name} (id, name, description, created, modified)
                VALUES {args}
                """)

    def genre_film_work_insert(self, cursor, pg_conn):
        print('genre_film_work_insert')

    def person_film_work_insert(self, cursor, pg_conn):
        print('person_film_work_insert')

    def person_insert(self, cursor, pg_conn):
        print('person_insert')

    def film_work_insert(self, cursor, pg_conn):
        print('film_work_insert')

def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    # postgres_saver = PostgresSaver(pg_conn)
    # sqlite_extractor = SQLiteExtractor(connection)
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables[:1]:
        if table[0] in TABLE_NAME_METHODS:
            getattr(
                Table(table[0]),
                TABLE_NAME_METHODS[table[0]]
            )(cursor, pg_conn)
    # data = sqlite_extractor.extract_movies()
    # postgres_saver.save_all_data(data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)