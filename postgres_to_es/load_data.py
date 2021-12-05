import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import config as conf
from es import ElasticsearchLoader
from postgres_loader import PostgresData
from state import State, JsonFileStorage


def main(pg_connection: _connection) -> None:
    state = State(JsonFileStorage("PostgresStorages.json"))
    db_data = PostgresData(pg_connection, state).get_data()  # Получаем данные из базы

    es = ElasticsearchLoader(conf.es_dsl, 'movies', 'es_schema/index.json', state)
    es.create_index()
    es.load_es_data(db_data)  # Загружаем данные в Elasticsearch


if __name__ == '__main__':
    try:
        with psycopg2.connect(**conf.db_dsl, cursor_factory=DictCursor) as pg_conn:
            main(pg_conn)
    finally:
        pg_conn.close()
