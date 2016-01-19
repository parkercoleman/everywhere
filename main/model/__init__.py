import pg8000
from main import DEFAULT_LOGGER
from main.config.db_config import db_config
from shapely.geometry import Point


def get_connection():
    return pg8000.connect(user=db_config["user"],
                      host=db_config["host"],
                      port=db_config["port"],
                      database=db_config["db_name"],
                      password=db_config["password"])


def with_pg_connection(function):
    def wrapper(*args, **kwargs):
        conn = None
        c = None
        try:
            # The calling function might want to supply the connections themselves and close them later.
            if 'connection' not in kwargs:
                conn = get_connection()
                kwargs['connection'] = conn

            if 'cursor' not in kwargs:
                c = kwargs['connection'].cursor()
                kwargs['cursor'] = c

            return function(*args, **kwargs)
        except Exception as e:
            DEFAULT_LOGGER.error("Error running DB query: " + str(e))
        finally:
            if c is not None:
                c.close()

            if conn is not None:
                conn.close()

    return wrapper


def get_node_name_from_location(lat, lon):
    return get_node_name_from_location(Point(lon, lat))


def get_node_name_from_location(shapely_point):
    return str(shapely_point.y) + "," + str(shapely_point.x)
