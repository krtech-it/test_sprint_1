import os
from datetime import datetime, timezone

from dotenv import load_dotenv


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
COUNT_FETCHMANY = 200
