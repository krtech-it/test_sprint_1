import sqlite3
from contextlib import contextmanager


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()

db_path = 'db.sqlite'

with conn_context(db_path) as conn:
    curs = conn.cursor()
    curs.execute("select * from film_work;")
    data = curs.fetchall()
    print(dict(data[0]))
