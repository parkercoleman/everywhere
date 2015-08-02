import pg8000
from config.db_config import db_config
__author__ = 'pcoleman'

def get_connection():
    return pg8000.connect(user=db_config["user"],
                      host=db_config["host"],
                      port=db_config["port"],
                      database=db_config["db_name"],
                      password=db_config["password"])