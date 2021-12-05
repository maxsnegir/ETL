import os
from dotenv import load_dotenv

load_dotenv()

db_dsl = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
}

es_dsl = {
    'host': os.environ.get('ELASTIC_HOST'),
    'port': os.environ.get('ELASTIC_PORT')
}
