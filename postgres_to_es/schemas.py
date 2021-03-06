from pydantic import BaseModel
from typing import Union, List, Dict
from uuid import UUID


class FilmWork(BaseModel):
    id: Union[str, UUID]
    title: Union[str, None]
    description: Union[str, None]
    imdb_rating: Union[float, None]
    genre: Union[List[Dict[Union[str, UUID], str]], None]

    actors: Union[List[Dict[Union[str, UUID], str]], None]
    director: Union[List[Dict[Union[str, UUID], str]], None]
    writers: Union[List[Dict[Union[str, UUID], str]], None]

    actors_names: Union[List[str], None]
    directors_names: Union[List[str], None]
    writers_names: Union[List[str], None]
