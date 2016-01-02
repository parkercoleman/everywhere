from src.model import with_pg_connection
import shapely.wkt


class RoadsDAO:
    @staticmethod
    @with_pg_connection
    def get_road_geom(road_id, **kwargs):
        c = kwargs['cursor']
        sql_string = """SELECT ST_AsText(geom) FROM gis.roads r WHERE r.linearid = '{0}' """.format(road_id)
        c.execute(sql_string)
        results = c.fetchone()
        return shapely.wkt.loads(results[0]) if results is not None else None


