__author__ = 'pcoleman'

from collections import defaultdict
from src import DEFAULT_LOGGER
import networkx as nx
import pickle
import os

from src.dao import get_connection


class RoadGraph:
    def __init__(self):
        self.graph = nx.Graph()

    def save(self, pickle_file_name):
        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(self, pfile, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(pickle_file_name):
        with open(pickle_file_name, 'rb') as pfile:
            return pickle.load(pfile)

    @staticmethod
    def __gather_road_data():
        if os.path.exists("roads_info.pickle") and os.path.exists("roads_to_nodes.pickle"):
            DEFAULT_LOGGER.info("roads_to_nodes.pickle and roads_info.pickle were found, loading data from these two files")
            return RoadGraph.load("roads_info.pickle"), RoadGraph.load("roads_to_nodes.pickle")

        # Two queries that dump out our results into giant temporary tables

        road_intersection_query = """
            SELECT  r1.linearid AS r1id,
                    r1.fullname AS r1name,
                    r2.linearid AS r2id,
                    r2.fullname AS r2name,
                    ST_AsText(ST_Centroid(ST_Intersection(r1.geom, r2.geom))) AS intersection_point,
                    ST_Length(r1.geom, true) AS r1len,
                    ST_Length(r2.geom, true) AS r2len
            INTO TABLE roads_intersection
            FROM roads r1
            INNER JOIN roads r2 ON ((ST_Touches(r1.geom, r2.geom) OR ST_Intersects(r1.geom, r2.geom))
                AND r1.linearid != r2.linearid
                AND r2.rttyp NOT IN ('I')
                AND NOT ST_Equals(r1.geom, r2.geom))
            WHERE r1.rttyp NOT IN ('I')
        """

        places_intersection_query = """
            WITH max_length AS (
                SELECT p.gid, MAX(ST_Length(ST_Intersection(p.geom, r.geom), true)) as max_length
                FROM places p
                INNER JOIN roads r ON ST_Intersects(p.geom, r.geom)
                WHERE p.lsad = '25'
                AND r.rttyp NOT IN ('I')
                GROUP BY p.gid
            )

            SELECT p.gid, statefp, placens, name, r.linearid, r.fullname,
                ST_AsText(ST_Centroid(ST_Intersection(p.geom, r.geom))) as location
            INTO TABLE places_intersection
            FROM places p
            INNER JOIN roads r ON ST_Intersects(p.geom, r.geom)
            INNER JOIN max_length ml ON (ST_Length(ST_Intersection(p.geom, r.geom), true) = ml.max_length AND p.gid = ml.gid)
            WHERE p.lsad = '25'
            AND r.rttyp NOT IN ('I')
            """

        roads_info = {}
        roads_to_nodes = defaultdict(list)

        def create_temp_tables():
            e_conn = get_connection()
            try:
                e_conn.autocommit = True
                DEFAULT_LOGGER.info("Executing Road Intersection Query, this may take up to an hour on a slow computer")
                e_conn.cursor().execute("DROP TABLE IF EXISTS roads_intersection")
                e_conn.cursor().execute(road_intersection_query)
                e_conn.cursor().execute("DROP TABLE IF EXISTS places_intersection")
                DEFAULT_LOGGER.info("Executing Places Intersection Query, this make take up to 30 minutes on a slow computer")
                e_conn.cursor().execute(places_intersection_query)
            finally:
                e_conn.close()

        def get_tuple_from_point_text(point_text):
            istring = point_text.replace("POINT(", "")
            lon = float(istring[:istring.index(' ')])
            lat = float(istring[istring.index(' ')+1:-1])
            return lat, lon

        def get_intersection_name(r1id, r2id):
            s = [str(r1id), str(r2id)]
            s.sort()
            return "_".join(s)

        create_temp_tables()
        conn = get_connection()
        c = conn.cursor()

        for i in range(0, 100000000, 100000):
            DEFAULT_LOGGER.info("Gathering road information OFFSET {0}".format(str(i)))
            c.execute("SELECT * FROM roads_intersection ORDER BY r1id, r2id LIMIT 100000 OFFSET {0}".format(str(i)))
            results = c.fetchall()
            have_results = False
            for row in results:
                have_results = True
                r1id, r1name, r2id, r2name, intersection_point, r1len, r2len = row
                r1id = int(r1id)
                r2id = int(r2id)

                roads_info[r1id] = {"id": r1id, "name": r1name, "weight": r1len}
                roads_info[r2id] = {"id": r2id, "name": r2name, "weight": r2len}

                node_lat, node_lon = get_tuple_from_point_text(intersection_point)
                node = {"id": get_intersection_name(r1id, r2id), "lat": node_lat, "lon": node_lon}
                roads_to_nodes[r1id].append(node)
                roads_to_nodes[r2id].append(node)

            if not have_results:
                break

        # Now we need to calculate the "City Nodes", which represent cities and are attached to roads.  The Largest
        # Intersection of a Road Line/City polygon decides which road a city node is on
        # (in reality they're usually on many)

        DEFAULT_LOGGER.info("Gathering places information".format(str(i)))
        c.execute("SELECT * FROM places_intersection".format(str(i)))
        results = c.fetchall()
        for row in results:
            gid, statefp, placens, name, rid, rname, location = row
            node_lat, node_lon = get_tuple_from_point_text(location)
            # Create a city node, similar to an intersection node, but with different tags (namely a "city_name")
            roads_to_nodes[int(rid)].append({"id": gid, "city_name": name, "lat": node_lat, "lon": node_lon})

        # Write out these results to disk
        for ftuple in (("roads_info.pickle", roads_info), ("roads_to_nodes.pickle", roads_to_nodes)):
            with open(ftuple[0], 'wb') as pfile:
                pickle.dump(ftuple[1], pfile, protocol=pickle.HIGHEST_PROTOCOL)

        return roads_info, roads_to_nodes

    @staticmethod
    def construct_graph(pickle_file_name):
        roads_info, roads_to_nodes = RoadGraph.__gather_road_data()
        # Now we start actually building the graph.  If a road has multiple nodes attached to it,
        # that means those nodes are connected.  We will use the road's length as the weight of that connection.
        # This isn't always strictly correct, but it should be close enough.
        r = RoadGraph()

        DEFAULT_LOGGER.info("Creating Graph Data Structure")
        for road in roads_to_nodes:
            nodes = roads_to_nodes[road]
            # make all the nodes
            for n in nodes:
                r.graph.add_node(n["id"], attr_dict=n)

            # make all the edges
            if road in roads_info:
                for i in range(0, len(nodes)):
                    for j in range(i, len(nodes)):
                        r.graph.add_edge(nodes[i]["id"], nodes[j]["id"], attr_dict=roads_info[road])
            else:
                DEFAULT_LOGGER.warn("Road ID {0} Not found in Roads Info, cannot make an edge out of it".format(road))

        r.save(pickle_file_name)
        return r


if __name__ == "__main__":
    r = RoadGraph.construct_graph("test.pickle")
    # r = RoadGraph.load("test.pickle")

    print("Graph contains {0} nodes".format(nx.number_of_nodes(r.graph)))
    gi = nx.nodes_iter(r.graph)

    for n, d in r.graph.nodes_iter(data=True):
        if "city_name" in d:
            print("{0} {1}". format(str(n), str(d)))



    r.graph.shortest_path(source="1565", target="1582")









