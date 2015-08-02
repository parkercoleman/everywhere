__author__ = 'pcoleman'

from database import get_connection
from collections import defaultdict


class RoadNode:
    def __init__(self, id, lat, lon):
        self.id = id
        self.lat = lat
        self.lon = lon

    @staticmethod
    def create_node_from_db_results(r1id, r2id, intersection_point_str):
        s = [r1id, r2id]
        s.sort()
        node_id = "_".join(s)
        istring = intersection_point_str.replace("POINT(")
        lon = float(istring[:istring.index(' ')])
        lat = float(istring[istring.index(' ')+1:-1])
        return RoadNode(node_id, lat, lon)


class RoadGraph:
    def __init__(self):
        self.road_lengths ={}
        self.roads_to_nodes = {}

    @staticmethod
    def construct_graph(pickle_file_name):
        # Grouping this by the ending of linearid (01, 02, 03... etc) I know you can use LIMIT/OFFSET but I didn't trust
        # it unless I could order both by r1.linearid AND r2.linearid and that turned out to be computationally
        # Expensive
        road_intersection_query = """
            SELECT  r1.linearid AS r1id,
                    r1.fullname AS r1name,
                    r2.linearid AS r2id,
                    r2.fullname AS r2name,
                    ST_AsText(ST_Centroid(ST_Intersection(r1.geom, r2.geom))) AS intersection_point,
                    ST_Length(r1.geom, true) AS r1len,
                    ST_Length(r2.geom, true) AS r2len
            FROM roads r1
            INNER JOIN roads r2 ON ((ST_Touches(r1.geom, r2.geom) OR ST_Intersects(r1.geom, r2.geom))
                AND r1.linearid != r2.linearid
                AND r2.rttyp NOT IN ('M', 'I')
                AND NOT ST_Equals(r1.geom, r2.geom))

            WHERE r1.rttyp NOT IN ('M', 'I')
            AND r1.linearid LIKE '%{0}'
        """

        r = RoadGraph()
        r.road_lengths = {}
        r.roads_to_nodes = defaultdict(list)

        conn = get_connection()
        c = conn.cursor()
        for i in range(0, 100):
            c.execute(road_intersection_query.format(str(i).zfill(2)))
            r1id, r1name, r2id, r2name, intersection_point, r1len, r2len = c.fetchall()
            r.roads_info[r1id] = r1len
            r.roads_info[r2id] = r2len

            node = RoadNode.create_node_from_db_results(r1id, r2id, intersection_point)
            r.roads_to_nodes[r1id].append(node)
            r.roads_to_nodes[r2id].append(node)





