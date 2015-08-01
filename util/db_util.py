__author__ = 'pcoleman'

import pg8000
from config.db_config import db_config


def get_connection():
    return pg8000.connect(user=db_config["user"],
                      host=db_config["host"],
                      port=db_config["port"],
                      database=db_config["db_name"],
                      password=db_config["password"])


def create_tables():
    conn = get_connection()
    try:
        places_sql = """
            DROP TABLE IF EXISTS "places";
            CREATE TABLE "places" (gid serial,
            "statefp" varchar(2),
            "placefp" varchar(5),
            "placens" varchar(8),
            "geoid" varchar(7),
            "name" varchar(100),
            "namelsad" varchar(100),
            "lsad" varchar(2),
            "classfp" varchar(2),
            "pcicbsa" varchar(1),
            "pcinecta" varchar(1),
            "mtfcc" varchar(5),
            "funcstat" varchar(1),
            "aland" float8,
            "awater" float8,
            "intptlat" varchar(11),
            "intptlon" varchar(12));
            ALTER TABLE "places" ADD PRIMARY KEY (gid);
            SELECT AddGeometryColumn('public','places','geom','4269','MULTIPOLYGON',2);
        """
        roads_sql = """
            DROP TABLE IF EXISTS "roads";
            CREATE TABLE "public"."roads" (gid serial,
            "linearid" varchar(22),
            "fullname" varchar(100),
            "rttyp" varchar(1),
            "mtfcc" varchar(5));
            ALTER TABLE "public"."roads" ADD PRIMARY KEY (gid);
            SELECT AddGeometryColumn('public','roads','geom','4269','MULTILINESTRING',2);
        """

        for sql in (places_sql, roads_sql):
            for c in sql.split(";"):
                if not c.strip() == "":
                    conn.cursor().execute(c)
                    conn.commit()

    finally:
        conn.close()


def execute_import_statements(file_name):
        conn = get_connection()
        c = conn.cursor()
        try:
            with open(file_name, 'r') as f:
                sql_lines = f.readlines()
                i = 0
                for sql_line in sql_lines:
                    if not sql_line.startswith("INSERT"):
                        continue

                    c.execute(sql_line)
                    if i % 50 == 0:
                        conn.commit()

            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    execute_import_statements("/media/sf_E_DRIVE/projects/everywhere/data/places/temp.txt")
