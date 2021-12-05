class FilmWorkQuery:
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
    """

    END_QUERY_BASE = """ GROUP BY fw.id ORDER BY fw.updated_at"""
    END_QUERY_WITH_FILTER = """ WHERE fw.updated_at > '{}' GROUP BY fw.id ORDER BY fw.updated_at"""
