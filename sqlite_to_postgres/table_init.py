from dataclasses import dataclass, fields
from datetime import datetime

from sqlite_work import SQLiteExtractor
from settings import TIMESTAMP_WITH_TIMEZONE


class TableWork:

    def sorted_values(self, headers: list) -> tuple:
        return tuple(self.__dict__.get(colm) for colm in headers)

    @classmethod
    def create_headers_list(cls,
                            sqlite_extractor: SQLiteExtractor,
                            table: str) -> tuple:
        headers = sqlite_extractor.get_headers(table)
        table_class_headers = set(map(lambda x: x.name, fields(cls)))
        headers = headers.intersection(table_class_headers)
        return headers

    @classmethod
    def create_data_for_insert(cls,
                               headers: set,
                               data: list) -> dict:
        for n, table_data in enumerate(data):
            table_data = dict(zip(tuple(headers), table_data))
            table_class = cls(**table_data)
            data[n] = (table_class.sorted_values(headers))
        return data


@dataclass
class FilmWork(TableWork):
    id: str
    title: str
    description: str
    creation_date: str
    rating: str
    type: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE
    updated_at: datetime = TIMESTAMP_WITH_TIMEZONE

    def __post_init__(self):
        self.created_at: datetime = TIMESTAMP_WITH_TIMEZONE
        self.updated_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class Genre(TableWork):
    id: str
    name: str
    description: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE
    updated_at: datetime = TIMESTAMP_WITH_TIMEZONE

    def __post_init__(self):
        self.created_at: datetime = TIMESTAMP_WITH_TIMEZONE
        self.updated_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class Person(TableWork):
    id: str
    full_name: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE
    updated_at: datetime = TIMESTAMP_WITH_TIMEZONE

    def __post_init__(self):
        self.created_at: datetime = TIMESTAMP_WITH_TIMEZONE
        self.updated_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class GenreFilmWork(TableWork):
    id: str
    film_work_id: str
    genre_id: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE

    def __post_init__(self):
        self.created_at: datetime = TIMESTAMP_WITH_TIMEZONE


@dataclass
class PersonFilmWork(TableWork):
    id: str
    film_work_id: str
    person_id: str
    role: str
    created_at: datetime = TIMESTAMP_WITH_TIMEZONE

    def __post_init__(self):
        self.created_at: datetime = TIMESTAMP_WITH_TIMEZONE


TABLE_NAME_CLASSES = {
    'genre': Genre,
    'genre_film_work': GenreFilmWork,
    'person_film_work': PersonFilmWork,
    'person': Person,
    'film_work': FilmWork,
}
