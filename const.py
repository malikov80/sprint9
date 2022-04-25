# Данные загружаются пачками максимум по NUMBER_OF_ROWS записей.
NUMBER_OF_ROWS = 500

SQL_QUERIES = {'person_up': {'initial': """SELECT id, updated_at FROM content.person 
            WHERE updated_at > '{}';""",

                             'middle': """SELECT
        fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN {}
        ORDER BY fw.updated_at;
        """,

                             'addall': """SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created_at,
            fw.updated_at,
            pfw.role,
            p.id,
            p.full_name,
            g.name,
            g.id
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre
            WHERE fw.id IN {} ORDER BY fw.id;
            """},
               'film_up': {'initial': """SELECT id, updated_at FROM content.film_work
                    WHERE updated_at > '{}';""",
                           'middle': '',
                           'addall': """SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created_at,
            fw.updated_at,
            pfw.role,
            p.id,
            p.full_name,
            g.name,
            g.id
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre
            WHERE fw.id IN {}
            ORDER BY fw.id;
            """},

               'genre_up': {'initial': """SELECT id, updated_at FROM content.genre
                       WHERE updated_at > '{}';""",
                            'middle': """SELECT
                       fw.id, fw.updated_at
                       FROM content.film_work fw
                       LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                       WHERE gfw.genre IN {}
                       ORDER BY fw.updated_at;
                       """,
                            'addall': """SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created_at,
            fw.updated_at,
            pfw.role,
            p.id,
            p.full_name,
            g.name,
            g.id
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre
            WHERE fw.id IN {}
            ORDER BY fw.id;
            """}
               }
SQL_QUERIES_PERSON = {'initial': """SELECT id, full_name, birth_date, updated_at FROM content.person 
            WHERE updated_at > '{}' ORDER BY updated_at;"""}
SQL_QUERIES_GENRE = {'initial': """SELECT id, name, description, updated_at FROM content.genre 
            WHERE updated_at > '{}' ORDER BY updated_at;"""}
