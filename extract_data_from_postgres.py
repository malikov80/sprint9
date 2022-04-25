import json
from time import sleep

import environ
import psycopg2
import psycopg2.extras
import requests

import save_load_state
from const import NUMBER_OF_ROWS, SQL_QUERIES, SQL_QUERIES_PERSON, SQL_QUERIES_GENRE
from log import logger
from film import film_index
from person import person_index
from genre import genre_index


env_root = environ.Path(__file__) - 2
env = environ.Env()
env_file = str(env_root.path('dev.env'))
env.read_env(env_file)
# Ссылка на Elasticsearch
ES_URL = 'http://'+env('ELASTIC_HOST')+':'+env('ELASTIC_PORT')

def set_elastic_index():
    try:
        resp1 = requests.head(ES_URL+'/movies',
                     headers={'content-type': 'application/json', 'charset': 'UTF-8'})
        resp2 = requests.head(ES_URL+'/person',
                     headers={'content-type': 'application/json', 'charset': 'UTF-8'})
        resp3 = requests.head(ES_URL+'/genre',
                     headers={'content-type': 'application/json', 'charset': 'UTF-8'})
        if resp1.ok and resp2.ok and resp3.ok:
            return True
        if not resp1.ok :
            resp1 = requests.put(ES_URL + '/movies', data=json.dumps(film_index),
                              headers={'content-type': 'application/json', 'charset': 'UTF-8'})
            return True
        if not resp2.ok:
            resp2 = requests.put(ES_URL + '/person', data=json.dumps(person_index),
                                 headers={'content-type': 'application/json', 'charset': 'UTF-8'})
            return True
        if not resp3.ok:
            resp3 = requests.put(ES_URL + '/genre', data=json.dumps(genre_index),
                                 headers={'content-type': 'application/json', 'charset': 'UTF-8'})
            return True
        return False
    except Exception as e:
        logger.exception('Elasticsearch error', e)
        return False


def extras(pg_conn: psycopg2.extensions.connection, sql_query: str, add: list):
    """ Обращение к базе postgres"""
    cursor = pg_conn.cursor()
    cursor.execute(sql_query.format(add))
    return cursor


def add_full_info(pg_conn: psycopg2.extensions.connection, sql_query: str, add: list, name_state: str, timestamp: str, fullresult) -> list:
    """ Обогащение данными"""
    cursor = extras(pg_conn, sql_query, add)
    while True:
        result = cursor.fetchmany(size=NUMBER_OF_ROWS)
        fullresult = fullresult+result
        current_state.set_state(
            name_state, timestamp)
        logger.info("Записано состояние {} по {}".format(
            name_state, timestamp))
        if len(result) < NUMBER_OF_ROWS:
            break
    return fullresult


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        def inner(*args, **kwargs):
            t = start_sleep_time
            while t <= border_sleep_time:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.exception('Database error', e)
                    t = t*factor
                    sleep(t)
        return inner
    return func_wrapper


def transform_data(rows: list) -> dict:
    """ Подготовка данных к отправке в Elasticsearch"""
    def add_roles_genres(old: dict, new: list) -> dict:
        """ Добавление данных в поля genre, actors, writers, director,
        actors_names, writers_names """

        if not {'name': new[-2], 'id': new[-1]} in old['genre']:
            old['genre'].append({'name': new[-2], 'id': new[-1]})
        if not new[-5] is None:
            if new[-5] == 'director':
                old['directors'] = [{'id': new[-4], 'name': new[-3]}]
            else:
                if not {'id': new[-4], 'name': new[-3]} in old[new[-5]+'s']:
                    old[new[-5]+'s'].append({'id': new[-4], 'name': new[-3]})
                if old[new[-5]+'s_names'] is None:
                    old[new[-5]+'s_names'] = [new[-3]]
                if not new[-3] in old[new[-5]+'s_names']:
                    old[new[-5]+'s_names'].append(new[-3])
        return old

    def init_names(row: list) -> dict:
        """ Создание словаря с пустыми агрегатными полями"""
        result: dict = {
            "id": row[0],
            "imdb_rating": row[3],
            "genre": [],
            "title": row[1],
            "description": row[2],
            "directors": None,
            "actors_names": None,
            "writers_names": None,
            "actors": [],
            "writers": []}
        return add_roles_genres(result, row)
    rows.sort(key=lambda x: x[0])
    result = []
    result.append(init_names(rows[0]))
    i = 0
    while i < len(rows)-1:
        i += 1
        if rows[i][0] == rows[i-1][0]:
            result[-1] = add_roles_genres(result[-1], rows[i])
        else:
            result.append(init_names(rows[i]))
    return result


@backoff(0.2, 2, 10)
def load_to_ES(normal: list, name: str, state: save_load_state.State):
    """ Выгрузка данных в Elasticsearch"""
    curstate = state.get_state(name)
    if curstate is None:
        i = 0
    else:
        for n, row in enumerate(normal):
            if row['id'] == curstate:
                i = n
                break
    while i < len(normal):
        start = i
        i += NUMBER_OF_ROWS
        if i > len(normal):
            i = len(normal)
        string_for_bulk = '\n'.join([json.dumps({'index': {
                                    '_index': 'movies', '_id': res['id']}})+'\n'+json.dumps(res) for res in normal[start:i]])+'\n'
        requests.post(ES_URL+'/_bulk', data=string_for_bulk, headers={
            'content-type': 'application/json', 'charset': 'UTF-8'})
        state.set_state(name, normal[i-1]['id'])
    state.set_state(name, None)
    logger.info('Загружено в ElasticSearch {} записей'.format(len(normal)))


@backoff(0.2, 2, 10)
def extract_data(pg_conn: psycopg2.extensions.connection, state: save_load_state.State, query: dict, name: str, previous_extract: list) -> list:
    """ Выгрузка данных из Postgres"""
    fullresult = []
    startdate = state.get_state(name)
    if startdate is None or startdate == {}:
        startdate = '1900-01-01 00:00:00.000000+00'
    cursor = extras(pg_conn, query['initial'], startdate)
    while True:
        initial_list = cursor.fetchmany(size=NUMBER_OF_ROWS)
        # Добавление ID фильмов
        if not initial_list:
            return []
        if query['middle'] != '':
            tuple_initial_list = tuple([row[0] for row in initial_list])
            middle_list = extras(
                pg_conn, query['middle'], tuple_initial_list).fetchall()
        else:
            middle_list = initial_list
        # Оставляем только фильмы, не попавшие в предыдущую выборку из базы
        unique_list = []
        if previous_extract:
            for row in middle_list:
                if not any(r[0] == row[0] for r in previous_extract):
                    unique_list.append(row)
        else:
            unique_list = middle_list
            # Добавление данных по фильмам
        if unique_list:
            filmworks = tuple([fw[0] for fw in unique_list])
            fullresult = add_full_info(pg_conn, query['addall'],
                                       filmworks, name, str(initial_list[-1][1]), fullresult)
        if len(initial_list) < NUMBER_OF_ROWS:
            break
    logger.info("Из базы {}скачано {}  уникальных полей".format(
        name, len(fullresult)))
    return fullresult+previous_extract


@backoff(0.2, 2, 10)
def extract_p_or_g(pg_conn: psycopg2.extensions.connection, state: save_load_state.State, query: dict, name: str) -> list:
    """ Выгрузка данных из Postgres для людей и жанров"""
    fullresult = []
    startdate = state.get_state(name)
    if startdate is None or startdate == {}:
        startdate = '1900-01-01 00:00:00.000000+00'
    cursor = extras(pg_conn, query['initial'], startdate)
    while True:
        initial_list = cursor.fetchmany(size=NUMBER_OF_ROWS)
        if not initial_list:
            return fullresult
        fullresult = fullresult + initial_list
        state.set_state(name, str(fullresult[-1][-1]))
        if len(initial_list) < NUMBER_OF_ROWS:
            break
    logger.info("Из базы {}скачано {}  уникальных полей".format(
        name, len(fullresult)))
    return fullresult


def transform_data_person(rows: list) -> dict:
    """ Подготовка данных к отправке в Elasticsearch"""
    rows.sort(key=lambda x: x[0])
    return [{"id": row[0], "full_name": row[1], "birth_date": row[2]} for row in rows]


def transform_data_genre(rows: list) -> dict:
    """ Подготовка данных к отправке в Elasticsearch"""
    rows.sort(key=lambda x: x[0])
    return [{"id": row[0], "name": row[1], "description": row[2]} for row in rows]


@backoff(0.2, 2, 10)
def load_to_ES_person_genre(normal: list, name: str, state: save_load_state.State):
    """ Выгрузка данных в Elasticsearch"""
    curstate = state.get_state(name)
    if curstate is None:
        i = 0
    else:
        for n, row in enumerate(normal):
            if row['id'] == curstate:
                i = n
                break
    while i < len(normal):
        start = i
        i += NUMBER_OF_ROWS
        if i > len(normal):
            i = len(normal)
        string_for_bulk = '\n'.join([json.dumps({'index': {
                                    '_index': name[8:], '_id': res['id']}})+'\n'+json.dumps(res) for res in normal[start:i]])+'\n'
        requests.post(ES_URL+'/_bulk', data=string_for_bulk, headers={
            'content-type': 'application/json', 'charset': 'UTF-8'})
        state.set_state(name, normal[i-1]['id'])
    state.set_state(name, None)
    logger.info('Загружено в ElasticSearch {} записей'.format(len(normal)))


if __name__ == '__main__':
    if set_elastic_index() :
        JsonFS = save_load_state.JsonFileStorage('config.json')
        current_state = save_load_state.State(JsonFS)
        logger.info("Программа стартует")
        dsl = {'dbname': env('DB_NAME'), 'user': env('DB_USER'),
                'password': env('DB_PASSWORD'), 'host': env('DB_HOST'),
                'port': env('DB_PORT'), 'options': '-c search_path=content'}
        with psycopg2.connect(**dsl, cursor_factory=psycopg2.extras.DictCursor) as pg_conn:
    
            extract4 = extract_p_or_g(pg_conn, current_state,
                                      SQL_QUERIES_PERSON, 'postgres_person_only')
            load_to_ES_person_genre(transform_data_person(
                extract4), 'elastic_person', current_state)
    
            extract5 = extract_p_or_g(pg_conn, current_state,
                                      SQL_QUERIES_GENRE, 'postgres_genre_only')
            load_to_ES_person_genre(transform_data_genre(
                extract5), 'elastic_genre', current_state)
    
            extract = extract_data(pg_conn, current_state,
                                    SQL_QUERIES['person_up'], 'postgres_persons', [])
            extract2 = extract_data(pg_conn, current_state,
                                    SQL_QUERIES['film_up'], 'postgres_films', extract)
            extract3 = extract_data(pg_conn, current_state,
                                    SQL_QUERIES['genre_up'], 'postgres_genres', extract2)
            if extract3:
                load_to_ES(transform_data(extract3), 'elastic', current_state)
        pg_conn.close()
        
    
