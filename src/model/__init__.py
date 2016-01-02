__author__ = 'pcoleman'
import pg8000
from src import DEFAULT_LOGGER
from src.config.db_config import db_config


def get_connection():
    return pg8000.connect(user=db_config["user"],
                      host=db_config["host"],
                      port=db_config["port"],
                      database=db_config["db_name"],
                      password=db_config["password"])


def with_pg_connection(function):
    def wrapper(*args, **kwargs):
        conn = get_connection()
        try:
            kwargs['cursor'] = conn.cursor()
            kwargs['connection'] = conn
            return function(*args, **kwargs)
        except Exception as e:
            DEFAULT_LOGGER.error("Error running DB query: " + str(e))
        finally:
            conn.close()

    return wrapper