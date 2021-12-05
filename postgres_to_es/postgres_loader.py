from typing import List
from postgres_to_es.state import State
from sql_queries import FilmWorkQuery as FWQuery
from psycopg2.extensions import connection as _connection, cursor as _cursor
from schemas import FilmWork


class PostgresData:
    """ Класс для получения данных из бд """

    def __init__(self, connection: _connection, state: State):
        self.connection = connection
        self.cursor: _cursor = self.connection.cursor()
        self.batch_size = 100

        self.state = state
        self.state_key_name = 'updated_at'
        self.state_key = self.state.get_state(self.state_key_name)

    def get_query(self) -> str:
        """ Формирование sql запроса """

        main_query = FWQuery.START_QUERY
        if self.state_key:
            return main_query + FWQuery.END_QUERY_WITH_FILTER.format(self.state_key)
        return main_query + FWQuery.END_QUERY_BASE

    def get_data(self) -> List:
        """Метод получения данных из базы, пачками по 100 шт."""

        data = []
        count_rows = 0
        while True:
            query = self.get_query()
            self.cursor.execute(query)
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                data.append(FilmWork(**(dict(row))))

            updated_at_value = str(dict(rows[-1]).get("updated_at"))
            self.state.set_state(self.state_key_name, updated_at_value)
            self.state_key = updated_at_value
            count_rows += len(rows)

        print(f'Загружено {count_rows} записей')
        return data
