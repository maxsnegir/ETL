class PersonQuery:

    START_QUERY = """
                SELECT p.id, 
                       p.updated_at
                FROM content.person p
                """

    FILTER_QUERY = """ WHERE updated_at > '{}'"""
    END_QUERY = """ ORDER BY updated_at"""

    @staticmethod
    def get_query(updated_at):
        if updated_at:
            return PersonQuery.START_QUERY + PersonQuery.FILTER_QUERY.format(updated_at) + PersonQuery.END_QUERY
        return PersonQuery.START_QUERY + PersonQuery.END_QUERY


class GenreQuery:
    START_QUERY = """
        SELECT g.id, g.updated_at
        FROM content.genre g 
    """

    FILTER_QUERY = """ WHERE updated_at > '{}'"""
    END_QUERY = """ ORDER BY updated_at"""

    @staticmethod
    def get_query(updated_at):
        if updated_at:
            return GenreQuery.START_QUERY + GenreQuery.FILTER_QUERY.format(updated_at) + GenreQuery.END_QUERY
        return GenreQuery.START_QUERY + GenreQuery.END_QUERY


class FWBase:

    BASE_QUERY = """
            SELECT DISTINCT fw.id, fw.updated_at
            FROM content.film_work fw
            """
    END_QUERY = """ ORDER BY fw.updated_at;"""
    FILTER_UPDATE_AT = """ AND fw.updated_at > %s"""

    PERSON_JOIN = f"""{BASE_QUERY} 
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id 
                        WHERE pfw.person_id IN %s"""

    GENRE_JOIN = f"""{BASE_QUERY} 
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id 
                    WHERE gfw.genre_id IN %s"""

    @staticmethod
    def get_query(updated_at, linked_table):
        if linked_table == PersonQuery.__name__:
            start_query = FWBase.PERSON_JOIN
            updated_at_filter = FWBase.FILTER_UPDATE_AT
        elif linked_table == GenreQuery.__name__:
            start_query = FWBase.GENRE_JOIN
            updated_at_filter = FWBase.FILTER_UPDATE_AT
        else:
            start_query = FWBase.BASE_QUERY
            updated_at_filter = """ WHERE fw.updated_at > %s"""

        if updated_at:
            return start_query + updated_at_filter + FWBase.END_QUERY
        return start_query + FWBase.END_QUERY


class AllDataQuery:

    START_QUERY = """
            SELECT DISTINCT fw.id id,
                   fw.title title,
                   fw.description description,
                   fw.rating imdb_rating,
                   fw.updated_at updated_at,

                   ARRAY_AGG(DISTINCT jsonb_build_object('name', g.name, 'id', g.id)) AS genre,
                   ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role='director') director,
                   ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role='actor') actors,
                   ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role='writer') writers,

                   ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role='actor') actors_names,
                   ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role='director') directors_names,
                   ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role='writer') writers_names

        FROM content.film_work fw
            LEFT JOIN content.genre_film_work gfw on fw.id=gfw.film_work_id 
            LEFT JOIN content.person_film_work pfw on fw.id=pfw.film_work_id
            LEFT JOIN content.genre g on gfw.genre_id=g.id
            LEFT JOIN content.person p on pfw.person_id=p.id
        WHERE fw.id in %s
        GROUP BY fw.id 
        ORDER BY fw.updated_at
    """

    FILTER_BY_FW = """ WHERE fw.id in %s"""
    FILTER_BY_UPDATED_AT = """WHERE updated_at > %s"""

    @staticmethod
    def get_query():
        return AllDataQuery.START_QUERY
