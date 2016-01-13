import time
import uuid
from src.model import with_pg_connection


class UserRoutesDAO:
    @staticmethod
    @with_pg_connection
    def insert_route(route, **kwargs):
        c = kwargs['cursor']
        conn = kwargs['connection']
        epoch_time = int(time.time())
        route_id = str(uuid.uuid4())
        insert_sql = """
            INSERT INTO gis.user_routes VALUES(
                %s,
                %s,
                TIMESTAMP WITH TIME ZONE 'epoch' + %s * INTERVAL '1 second',
                ST_GeomFromText(%s, 4269)
            )
        """

        for i in range(0, len(route.steps)):
            step = route.steps[i]
            if step.next_edge_geom is not None:
                c.execute(insert_sql, (route_id, i, epoch_time, step.next_edge_geom.wkt))

            i += 1

        conn.commit()
        return route_id
