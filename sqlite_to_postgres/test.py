import sqlite3
from contextlib import contextmanager


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()

db_path = 'db.sqlite'

with conn_context(db_path) as conn:
    cur = conn.cursor()
    cur.execute('select * from film_work where id = "ffc3df9f-a17e-4bae-b0b6-c9c4da290fdd"')
    movie = cur.fetchone()
    print(movie)
    # curs = conn.cursor()
    # curs.execute("select * from film_work order by id limit 5;")
    # data = curs.fetchall()
    # for row in data:
    #     print(row[0])
    # print('asdasd  ', row[0])
    # curs.execute("select * from film_work where id > '' order by id limit 5;".format(id=row[0]))
    # data = curs.fetchall()
    # for row in data:
    #     print(row[0])
