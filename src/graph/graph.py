__author__ = 'pcoleman'

from collections import defaultdict
from src import DEFAULT_LOGGER

import pickle

from src.database import get_connection


class RoadGraph:
    def __init__(self):
        self.roads_info = {}
        self.roads_to_nodes = defaultdict(list)
        self.nodes_to_nodes = defaultdict(set)

    def save(self, pickle_file_name):
        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(self, pfile, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(pickle_file_name):
        with open(pickle_file_name, 'rb') as pfile:
            return pickle.load(pfile)

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
            AND r1.linearid LIKE '%%{0}'
        """

        r = RoadGraph()

        conn = get_connection()
        c = conn.cursor()
        for i in range(0, 100):
            DEFAULT_LOGGER.info("Creating graph for roads with linearids that end in {0}".format(str(i).zfill(2)))
            c.execute(road_intersection_query.format(str(i).zfill(2)))
            results = c.fetchall()

            for row in results:
                r1id, r1name, r2id, r2name, intersection_point, r1len, r2len = row
                r1id = int(r1id)
                r2id = int(r2id)

                r.roads_info[r1id] = RoadEdge(r1id, r1name, r1len)
                r.roads_info[r2id] = RoadEdge(r2id, r1name, r2len)

                node = RoadNode.create_node_from_db_results(r1id, r2id, intersection_point)
                r.roads_to_nodes[r1id].append(node)
                r.roads_to_nodes[r2id].append(node)

        # If a single road id has multiple nodes attached to it, that means those nodes are connected to each other
        for road in r.roads_to_nodes:
            for nodes in r.roads_to_nodes[road]:
                for n in nodes:
                    r.nodes_to_nodes[n] = r.nodes_to_nodes[n] | (set(nodes) - set(n))

        r.save(pickle_file_name)


class RoadEdge:
    def __init__(self, id, name, length):
        self.id = id
        self.name = name
        self.length = length


class RoadNode:
    def __init__(self, id, lat, lon):
        self.id = id
        self.lat = lat
        self.lon = lon

    @staticmethod
    def create_node_from_db_results(r1id, r2id, intersection_point_str):
        s = [str(r1id), str(r2id)]
        s.sort()
        node_id = "_".join(s)
        istring = intersection_point_str.replace("POINT(", "")
        lon = float(istring[:istring.index(' ')])
        lat = float(istring[istring.index(' ')+1:-1])
        return RoadNode(node_id, lat, lon)


if __name__ == "__main__":
    RoadGraph.construct_graph("test.pickle")






