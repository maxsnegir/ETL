import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import config as conf
from es import ElasticsearchLoader
from postgres_loader import BaseLoader, MovieLoader
from sql_queries import PersonQuery, GenreQuery, FWBase
from state import State, JsonFileStorage


def etl(pg_connection: _connection) -> None:

    # Для себя описал схему в ETLSchema.png
    state = State(JsonFileStorage("PostgresStorages.json"))
    es = ElasticsearchLoader(conf.es_dsl, 'movies', 'es_schema/index.json')
    es.create_index()

    BaseLoader(pg_connection, state, es, PersonQuery).load()  # Изменившиеся Персоны
    BaseLoader(pg_connection, state, es, GenreQuery).load()  # Изменившиеся Жанры
    MovieLoader(pg_connection, state, es, FWBase).load()  # Изменившиеся Фильмы


if __name__ == '__main__':
    try:
        with psycopg2.connect(**conf.db_dsl, cursor_factory=DictCursor) as pg_conn:
            etl(pg_conn)
    finally:
        pg_conn.close()
