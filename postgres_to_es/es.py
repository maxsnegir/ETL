import json
from typing import List, Generator

from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from helpers import backoff
from schemas import FilmWork
from state import State
from config import logger


class ElasticsearchLoader:
    """Класс для загрузки данных в es """

    def __init__(self, conf: List, index_name: str, index_file_path: str, state: State):
        self.es = Elasticsearch(conf)
        self.index_file_path = index_file_path
        self.index_name = index_name
        self.bulk_size = 100

        self.state = state
        self.state_key_name = 'count_doc_number'
        self.state_key = self.state.get_state(self.state_key_name) or 0

    def create_index(self) -> None:
        """ Cоздание индекса в es """

        if not self.es.indices.exists(index=self.index_name):
            with open(self.index_file_path) as f:
                index_body = json.load(f)
            self.es.indices.create(index=self.index_name, body=index_body)

    @backoff()
    def load_es_data(self, docs: List[FilmWork]) -> None:
        """ Загружаем данные в es пачками """

        count = 0
        while docs:
            docs_for_load = docs[:self.bulk_size]
            self.load(docs_for_load)
            docs = docs[self.bulk_size:]

            count += len(docs_for_load)
            state_key_val = self.state_key + self.bulk_size
            self.state.set_state(self.state_key_name, state_key_val)
            self.state_key = state_key_val

        logger.info(f"Загружено {count} документов в es")

    def load(self, data) -> None:
        """ Основной метод загрузки данных """
        bulk(self.es, self.gen_data(data), index=self.index_name)

    def gen_data(self, docs: List[FilmWork]) -> Generator:
        """ Формируем формат для bulk запроса """

        for doc in docs:
            yield {
                "_index": self.index_name,
                "_id": doc.id,
                **doc.dict()
            }
