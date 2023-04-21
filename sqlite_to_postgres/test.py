from load_data import PostgresSaver, SQLiteExtractor
from load_data import dsl, sqlite3_path
import unittest

TABLE_FIELDS = {
    'genre': 'id, name, description',
    'film_work': 'id, title, description, creation_date, rating, type',
    'genre_film_work': 'id, film_work_id, genre_id',
    'person': 'id, full_name',
    'person_film_work': 'id, film_work_id, role'

}

class TestTransferDataSQL(unittest.TestCase):
    def setUp(self) -> None:
        self.pg_conn = PostgresSaver(data=dsl)
        self.sqlite_conn = SQLiteExtractor(sqlite3_path)

    def test_diff_count_values_table(self):
        tables = self.sqlite_conn.get_list_table()

        for table in tables:
            table = table[0]
            self.sqlite_conn.cursor.execute("""SELECT COUNT(id) FROM {table_name}""".format(table_name=table))
            count_sql = self.sqlite_conn.cursor.fetchone()[0]
            with self.pg_conn.conn_pg.cursor() as pg_cursor:
                pg_cursor.execute("""SELECT COUNT(id) FROM content.{table_name}""".format(table_name=table))
                count_pg = pg_cursor.fetchone()[0]
            assert count_sql == count_pg

    def test_diff_values_table_genre(self):
        for table, fields in TABLE_FIELDS.items():
            self.sqlite_conn.cursor.execute("""SELECT {0}  FROM {1}""".format(fields, table))
            data_sql = self.sqlite_conn.cursor.fetchall()
            for item_sql in data_sql[:2]:
                with self.pg_conn.conn_pg.cursor() as pg_cursor:
                    pg_cursor.execute("""SELECT {0} FROM content.{1} WHERE id = '{2}'""".format(fields, table, item_sql[0]))
                    item_pg = pg_cursor.fetchall()[0]
                    assert set(item_pg) == set(item_sql)

    def tearDown(self) -> None:
        self.sqlite_conn.close()
        self.pg_conn.close_connect()