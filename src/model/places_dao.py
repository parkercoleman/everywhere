from src.model import get_connection
import shapely.wkt


class PlacesDAO:
    @staticmethod
    def get_place_geom(place_id):
        conn = get_connection()
        try:
            c = conn.cursor()
            sql_string = """SELECT ST_AsText(geom) FROM places p WHERE p.gid = '{0}' """.format(place_id)
            c.execute(sql_string)
            results = c.fetchone()
            return shapely.wkt.loads(results[0]) if results is not None else None

        finally:
            conn.close()

    @staticmethod
    def get_place_centroid(place_id):
        conn = get_connection()
        try:
            c = conn.cursor()
            sql_string = """SELECT ST_AsText(ST_Centroid(geom)) FROM places p WHERE p.gid = '{0}' """.format(place_id)
            c.execute(sql_string)
            results = c.fetchone()
            return shapely.wkt.loads(results[0]) if results is not None else None

        finally:
            conn.close()
