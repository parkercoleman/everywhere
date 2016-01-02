from src.model import get_connection
import shapely.wkt


class RoadsDAO:
    @staticmethod
    def get_road_geom(road_id):
        conn = get_connection()
        try:
            c = conn.cursor()
            sql_string = """SELECT ST_AsText(geom) FROM gis.roads r WHERE r.linearid = '{0}' """.format(road_id)
            c.execute(sql_string)
            results = c.fetchone()
            return shapely.wkt.loads(results[0]) if results is not None else None

        finally:
            conn.close()
