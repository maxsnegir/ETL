import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from es import ElasticsearchLoader
from postgres_loader import PostgresData
from state import State, JsonFileStorage
import config as conf


def main(pg_connection: _connection, state: State, es: ElasticsearchLoader) -> None:
    db_data = PostgresData(pg_connection, state).get_data()
    es.load_data(db_data)


if __name__ == '__main__':
    with psycopg2.connect(**conf.db_dsl, cursor_factory=DictCursor) as pg_conn:
        state = State(JsonFileStorage("PostgresStorages.json"))
        es = ElasticsearchLoader(conf.es_dsl, 'movies', 'es_schema/index.json', state)
        es.create_index()

        main(pg_conn, state, es)
