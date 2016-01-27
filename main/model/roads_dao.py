from main.model import with_pg_connection
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

    @staticmethod
    @with_pg_connection
    def get_road_hashmap(**kwargs):
        """
        Gets all road geoms from database.... this returns a pretty huge hashmap
        :return:
        """
        c = kwargs['cursor']
        sql_string = "SELECT linearid, ST_AsText(geom), rttyp FROM gis.roads"
        c.execute(sql_string)
        results = c.fetchall()
        r = {}
        for row in results:
            id, geom, rttyp = row
            r[id] = {'geom': shapely.wkt.loads(geom), 'type': rttyp}

        return r
