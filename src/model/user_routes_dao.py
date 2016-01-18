import time
import shapely.wkt
from src.model.graph import Route
from src.model.graph import Step
from src.model import with_pg_connection


class UserRoutesDAO:
    @staticmethod
    @with_pg_connection
    def insert_and_decorate_route(route, **kwargs):
        """
        Takes a route object and inserts it into the database.  The process of insertion adds some additional vaulues
        to the route.  The GEOM is not returned for steps though.

        :param route:
        :param kwargs:
        :return:
        """
        c = kwargs['cursor']
        conn = kwargs['connection']
        epoch_time = int(time.time())
        route_id = route.id

        for i in range(0, len(route.steps)):
            step = route.steps[i]

            if step.geom is not None:
                insert_sql = """
                    INSERT INTO gis.user_routes VALUES(
                        %s,
                        %s,
                        %s,
                        'STEP',
                        ST_GeomFromText('POINT({0} {1})', 4269),
                        ST_Length(ST_GeomFromText('{2}', 4269), true),
                        ST_Centroid(ST_GeomFromText('{2}', 4269)),
                        ST_Envelope(ST_GeomFromText('{2}', 4269)),
                        ST_GeomFromText('{2}', 4269),
                        TIMESTAMP WITH TIME ZONE 'epoch' + %s * INTERVAL '1 second'
                    )
                """.format(step.start_lon, step.start_lat, step.geom.wkt)

            else:
                insert_sql = """
                    INSERT INTO gis.user_routes VALUES(
                        %s,
                        %s,
                        %s,
                        'STEP',
                        ST_GeomFromText('POINT({0} {1})', 4269),
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        TIMESTAMP WITH TIME ZONE 'epoch' + %s * INTERVAL '1 second'
                    )
                """.format(step.start_lon, step.start_lat)

            c.execute(insert_sql, (route_id, i, step.name, epoch_time))
            i += 1

        # Now that the steps are inserted, we need to insert the Route itself.
        insert_route_sql = """
                    INSERT INTO gis.user_routes VALUES(
                        %s,
                        NULL,
                        %s,
                        'ROUTE',
                        ST_GeomFromText('POINT({0} {1})', 4269),
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        TIMESTAMP WITH TIME ZONE 'epoch' + %s * INTERVAL '1 second'
                    )
                """.format(route.start_lon, route.start_lat)

        c.execute(insert_route_sql, (route_id, route.name, epoch_time))
        conn.commit()

        # Now we need to update the inserted route with some aggregrate values from the steps
        c.execute("""
            UPDATE gis.user_routes
            SET geom_length_meters = agg_steps.length_meters,
                geom_centroid = agg_steps.centroid,
                geom_extent = agg_steps.envelope,
                geom = agg_steps.geom,
                last_accessed = agg_steps.last_accessed

            FROM (
                SELECT SUM(geom_length_meters) AS length_meters, ST_Centroid(ST_Union(geom)) AS centroid, ST_Envelope(ST_Union(geom)) AS envelope, ST_Linemerge(ST_Collect(geom)) AS geom, MIN(last_accessed) AS last_accessed
                FROM gis.user_routes
                WHERE route_id='{0}'
                AND entry_type = 'STEP'
                GROUP BY route_id
            ) AS agg_steps
            WHERE route_id = '{0}'
            AND entry_type = 'ROUTE'
        """.format(route_id))
        conn.commit()

        # Now we need to pull the values back out of the database, since we
        # have added data to them during the INSERT process
        c.execute("""SELECT route_id,
                        step_id,
                        step_name,
                        entry_type,
                        ST_AsText(starting_point),
                        geom_length_meters,
                        ST_AsText(geom_centroid),
                        ST_AsText(geom_extent)
                   FROM gis.user_routes WHERE route_id = '{0}'
                   ORDER BY entry_type, step_id ASC""".format(str(route_id)))

        steps = []
        route = None
        for row in c.fetchall():
            (route_id, step_id, step_name, entry_type, starting_point_str, geom_length_meters, geom_centroid_str, geom_extent_str) = row
            sp = shapely.wkt.loads(starting_point_str)
            gc = shapely.wkt.loads(geom_centroid_str) if geom_centroid_str is not None else None
            ge = shapely.wkt.loads(geom_extent_str) if geom_extent_str is not None else None

            if entry_type == "ROUTE":
                s = Route(route_id, sp.y, sp.x, step_name)
                route = s
                s.steps = steps
            else:
                s = Step(route_id, step_id, sp.y, sp.x, step_name, None)
                steps.append(s)

            s.distance_meters = geom_length_meters if geom_length_meters is not None else 0
            s.geom_centroid = gc
            s.geom_bbox = ge

        return route
