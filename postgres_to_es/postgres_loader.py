from typing import List

from es import ElasticsearchLoader
from state import State
from sql_queries import AllDataQuery, FWBase
from psycopg2.extensions import connection as _connection, cursor as _cursor
from schemas import FilmWork
from helpers import backoff


class BaseLoader:
    """Класс добавления в es фильмов у которых изменились персоны/жанры/аналогичные связанные объекты """

    def __init__(self, connection: _connection, state: State, es: ElasticsearchLoader, updated_table):
        self.connection = connection
        self.cursor: _cursor = self.connection.cursor()
        self.state = state
        self.es = es

        self.bulk_size = 100
        self.base_key = updated_table.__name__
        self.fw_key = 'fw_updated_at'
        self.updated_table = updated_table

    def get_updated_data(self) -> List:
        """ Получаем изменившиеся данные из переданной таблицы updated_table"""

        updated_at = self.state.get_state(self.base_key)
        query = self.updated_table.get_query(updated_at)
        data = self.execute_query(query)
        return data

    @backoff()
    def execute_query(self, query: str, *args) -> List:
        """ Выполнение sql запроса """

        self.cursor.execute(query, args)
        rows = self.cursor.fetchmany(self.bulk_size)
        return rows

    def get_all_data(self, fw_ids) -> List[FilmWork]:
        """ Получаем все данные фильмов по id"""

        query = AllDataQuery.get_query()
        rows = self.execute_query(query, fw_ids)
        movie_data = [FilmWork(**(dict(row))) for row in rows]
        return movie_data

    def get_fw_data(self, linked_ids):
        """ К изменившимся связанным данным в таблице джойним основную FilmWork """

        updated_at = self.state.get_state(self.fw_key)
        query = FWBase.get_query(updated_at, self.base_key)
        args = (linked_ids, updated_at) if updated_at else [linked_ids]
        fw_data = self.execute_query(query, *args)
        return fw_data

    def update_state_value(self, key, rows):
        """ Метод для обновления State по updated_at"""

        updated_at = str(dict(rows[-1]).get("updated_at"))
        self.state.set_state(key, updated_at)

    def get_rows_ids(self, rows):
        return tuple(str(dict(row).get("id")) for row in rows)

    def load(self):
        while True:
            updated_data = self.get_updated_data()
            if updated_data:
                linked_ids = self.get_rows_ids(updated_data)
                while True:
                    fw_data = self.get_fw_data(linked_ids)
                    if fw_data:
                        fw_ids = self.get_rows_ids(fw_data)
                        data = self.get_all_data(fw_ids)
                        self.es.load_es_data(data)
                        self.update_state_value(self.fw_key, fw_data)

                    else:
                        # Сбрасываем стейт для следующей пачки фильмов по тем же измененным данным
                        self.state.set_state(self.fw_key, None)
                        break
                self.update_state_value(self.base_key, updated_data)
            else:
                break


class MovieLoader(BaseLoader):
    """Класс добавления в es изменившихся фильмов"""

    def get_updated_data(self):
        updated_at = self.state.get_state(self.base_key)
        query = self.updated_table.get_query(updated_at, self.base_key)
        args = updated_at if updated_at else None
        data = self.execute_query(query, args)
        return data

    def load(self):
        while True:
            updated_data = self.get_updated_data()
            if updated_data:
                updated_fw_ids = self.get_rows_ids(updated_data)

                data = self.get_all_data(updated_fw_ids)
                self.es.load_es_data(data)
                self.update_state_value(self.base_key, updated_data)
            else:
                break
