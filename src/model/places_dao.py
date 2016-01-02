from src.model import with_pg_connection
import shapely.wkt
import us
import re


class PlacesDAO:
    @staticmethod
    @with_pg_connection
    def get_place_geom(place_id, **kwargs):
        c = kwargs['cursor']
        sql_string = """SELECT ST_AsText(geom) FROM gis.places p WHERE p.gid = '{0}' """.format(place_id)
        c.execute(sql_string)
        results = c.fetchone()
        return shapely.wkt.loads(results[0]) if results is not None else None

    @staticmethod
    @with_pg_connection
    def get_place_centroid(place_id, **kwargs):
        c = kwargs['cursor']
        sql_string = """SELECT ST_AsText(ST_Centroid(geom)) FROM gis.places p WHERE p.gid = '{0}' """.format(place_id)
        c.execute(sql_string)
        results = c.fetchone()
        return shapely.wkt.loads(results[0]) if results is not None else None

    @staticmethod
    @with_pg_connection
    def get_placeid_by_name(cityname, statename, **kwargs):
        c = kwargs['cursor']
        fips = us.states.lookup(statename).fips
        sql_string = """SELECT gid FROM gis.places WHERE name = %s and statefp = %s"""
        c.execute(sql_string, (cityname, str(fips)))
        results = c.fetchone()
        return results[0] if results is not None else None

    @staticmethod
    @with_pg_connection
    def get_city_and_state_from_partial(partial_cityname, **kwargs):
        c = kwargs['cursor']
        sql_string = "SELECT gid, name, statefp FROM gis.places WHERE lower(name) LIKE '{0}%%' ORDER BY name"\
            .format(re.sub(r'\W+', '', partial_cityname).lower())

        c.execute(sql_string)
        results = c.fetchall()
        if results is None:
            return None
        else:
            rl = []
            for r in results:
                rl.append({"gid": r[0], "city_name": r[1], "state_name": us.states.lookup(r[2]).name})
            return rl

