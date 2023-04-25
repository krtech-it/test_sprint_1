from dataclasses import dataclass

import psycopg2
from psycopg2.extras import DictCursor


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
