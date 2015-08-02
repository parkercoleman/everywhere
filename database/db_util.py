from database import get_connection

__author__ = 'pcoleman'


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
            CREATE INDEX places_geom_index ON places USING GIST (geom);
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
            CREATE INDEX roads_geom_index ON roads USING GIST (geom);
            CREATE INDEX roads_linearid_index ON roads(linearid);
        """

        for sql in (places_sql, roads_sql):
            for c in sql.split(";"):
                if not c.strip() == "":
                    conn.cursor().execute(c)
                    conn.commit()

    finally:
        conn.close()


def vacuum_full():
    conn = get_connection()
    try:
        conn.autocommit = True
        conn.cursor().execute("VACUUM FULL places")
        conn.cursor().execute("VACUUM FULL roads")
    finally:
        conn.close()


def execute_import_statements(sql_lines):
        conn = get_connection()
        c = conn.cursor()
        try:
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
