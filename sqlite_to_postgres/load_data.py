from sqlite_work import SQLiteExtractor
from pg_work import PostgresSaver
from settings import MANY_TO_MANY_TABLES
from settings import COUNT_FETCHMANY
from settings import dsl
from settings import sqlite3_path
from table_init import TABLE_NAME_CLASSES


def load_from_sqlite(sqlite_extractor, postgres_saver):
    """Основной метод загрузки данных из SQLite в Postgres"""
    list_table = sqlite_extractor.get_list_table()
    last_table = []
    for table in list_table:
        table = table[0]
        if table in TABLE_NAME_CLASSES:
            if table in MANY_TO_MANY_TABLES:
                last_table.append(table)
            else:
                last_table.insert(0, table)

    for table in last_table:
        headers = TABLE_NAME_CLASSES[table].create_headers_list(sqlite_extractor, table)
        cursor = sqlite_extractor.get_all_data(table, headers)
        while True:
            data = cursor.fetchmany(COUNT_FETCHMANY)
            if data:
                data = TABLE_NAME_CLASSES[table].create_data_for_insert(headers, data)
                postgres_saver.insert_data(headers=headers, data=data, table_name=table)
            else:
                break


if __name__ == '__main__':
    with PostgresSaver(dsl) as pg_conn, SQLiteExtractor(sqlite3_path) as sqlite_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
