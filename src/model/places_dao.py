from src.model import get_node_name_from_location, with_pg_connection
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
    def get_place_name_by_id(pid, **kwargs):
        c = kwargs['cursor']
        sql_string = 'SELECT "name", statefp FROM gis.places WHERE gid =' + str(pid)
        c.execute(sql_string)
        results = c.fetchone()
        return str(results[0]) + ", " + str(us.states.lookup(results[1]).name) if results is not None else None

    @staticmethod
    @with_pg_connection
    def get_nearest_intersection_from_point(lat, lon, **kwargs):
        if not ((type(lat) == int or type(lat) == float) and (type(lat) == int or type(lat) == float)):
            raise ValueError("Both lat and lon must be numeric")

        c = kwargs['cursor']
        intersection_lookup_sql = """
            WITH road_dis AS (
                SELECT intersection_point AS road_location, ST_Distance(ST_Transform(ST_GeomFromText('POINT(-91 30)', 4326), 4269), geom, true) AS road_distance
                FROM roads_intersection ri
                ORDER BY ST_Distance(ST_Transform(ST_GeomFromText('POINT({0} {1})', 4326), 4269), geom, true) ASC
                LIMIT 1
            ),
            place_dis AS(
                SELECT gid AS place_id, name AS place_name, location AS place_location, ST_Distance(ST_Transform(ST_GeomFromText('POINT(-91 30)', 4326), 4269), geom, true) AS place_distance
                FROM places_intersection pi
                ORDER BY ST_Distance(ST_Transform(ST_GeomFromText('POINT({0} {1})', 4326), 4269), geom, true) ASC
                LIMIT 1
            )

            SELECT r.*, p.*
            FROM road_dis r, place_dis p
        """.format(str(lon), str(lat))

        in_town_sql = """
            SELECT gid, name
            FROM gis.places p
            WHERE ST_Within(ST_Transform(ST_GeomFromText('POINT({0} {1})', 4326), 4269), p.geom)
        """.format(str(lon), str(lat))

        c.execute(intersection_lookup_sql)
        road_location, road_distance, place_id, place_name, place_location, place_distance = c.fetchone()

        c.execute(in_town_sql)
        row = c.fetchone()
        in_town_id, in_town_name = row if row is not None else (None, None)

        return {
            "node_id": get_node_name_from_location(shapely.wkt.road_location) if road_distance < place_distance else place_id,
            "nearest_town": place_name,
            "in_town_id": in_town_id,
            "in_town_name": in_town_name
        }

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

