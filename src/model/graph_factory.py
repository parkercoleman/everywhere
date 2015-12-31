from collections import defaultdict
from src import DEFAULT_LOGGER
import pickle
import os
from haversine import haversine
from src.model import get_connection
from src.model.graph import *


class GraphFactory:
    @staticmethod
    def save_graph(self, pickle_file_name):
        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(self, pfile, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load_graph(pickle_file_name):
        with open(pickle_file_name, 'rb') as pfile:
            return pickle.load(pfile)

    @staticmethod
    def __gather_road_data():
        if os.path.exists("roads_info.pickle") and os.path.exists("roads_to_nodes.pickle"):
            DEFAULT_LOGGER.info("roads_to_nodes.pickle and roads_info.pickle were found, "
                                "loading data from these two files")

            return GraphFactory.load_graph("roads_info.pickle"), GraphFactory.load_graph("roads_to_nodes.pickle")

        # Two queries that dump out our results into giant temporary tables
        road_intersection_query = """
            SELECT  r1.linearid AS r1id,
                    r1.fullname AS r1name,
                    r2.linearid AS r2id,
                    r2.fullname AS r2name,
                    ST_AsText(ST_Intersection(r1.geom, r2.geom)) AS intersection_point,
                    ST_Length(r1.geom, true) AS r1len,
                    ST_Length(r2.geom, true) AS r2len,
                    ST_Intersection(r1.geom, r2.geom) AS geom
            INTO TABLE roads_intersection
            FROM roads r1
            INNER JOIN roads r2 ON ((ST_Touches(r1.geom, r2.geom) OR ST_Intersects(r1.geom, r2.geom))
                AND GeometryType(ST_Intersection(r1.geom, r2.geom)) = 'POINT'
                AND r1.linearid != r2.linearid
                AND r2.rttyp NOT IN ('I')
                AND NOT ST_Equals(r1.geom, r2.geom))
            WHERE r1.rttyp NOT IN ('I')
        """

        max_len_query = """
            SELECT p.gid, MAX(ST_Length(ST_Intersection(p.geom, r.geom), true)) as max_length
            INTO TABLE temp_max_length
            FROM places p
            INNER JOIN roads r ON ST_Intersects(p.geom, r.geom)
            INNER JOIN roads_intersection ri ON (r.linearid = ri.r1id OR r.linearid = ri.r2id)
            WHERE p.lsad = '25'
            AND r.rttyp NOT IN ('I', 'M')
            GROUP BY p.gid
        """

        places_intersection_query = """
            SELECT p.gid, statefp, placens, name, r.linearid, r.fullname,
                ST_AsText(ST_Centroid(ST_Intersection(p.geom, r.geom))) as location,
                ST_Centroid(ST_Intersection(p.geom, r.geom)) as geom
            INTO TABLE places_intersection
            FROM places p
            INNER JOIN roads r ON ST_Intersects(p.geom, r.geom)
            INNER JOIN temp_max_length ml
                ON (ST_Length(ST_Intersection(p.geom, r.geom), true) = ml.max_length AND p.gid = ml.gid)
            WHERE p.lsad = '25'
            AND r.rttyp NOT IN ('I', 'M')
            """

        roads_info = {}
        roads_to_nodes = defaultdict(list)

        def create_temp_tables():
            commands = ["DROP TABLE IF EXISTS roads_intersection",
                        road_intersection_query,
                        "DROP TABLE IF EXISTS places_intersection",
                        "DROP TABLE IF EXISTS temp_max_length",
                        "CREATE INDEX ON roads_intersection(r1id)",
                        "CREATE INDEX ON roads_intersection(r2id)",
                        "VACUUM FULL",
                        max_len_query,
                        "CREATE INDEX ON temp_max_length(gid)",
                        "CREATE INDEX ON temp_max_length(max_length)",
                        "VACUUM FULL",
                        places_intersection_query]

            DEFAULT_LOGGER.info("Creating places and roads intersection tables, this can take hours on a slow machine")
            e_conn = get_connection()
            e_conn.autocommit = True
            try:
                for command in commands:
                    DEFAULT_LOGGER.info("Executing: " + command)
                    e_conn.cursor().execute(command)
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
                r1id, r1name, r2id, r2name, intersection_point, r1len, r2len, geom = row
                r1id = int(r1id)
                r2id = int(r2id)

                roads_info[r1id] = {"id": r1id, "name": r1name}
                roads_info[r2id] = {"id": r2id, "name": r2name}

                node_lat, node_lon = get_tuple_from_point_text(intersection_point)
                node = {"id": get_intersection_name(r1id, r2id), "lat": node_lat, "lon": node_lon}
                roads_to_nodes[r1id].append(node)
                roads_to_nodes[r2id].append(node)

            if not have_results:
                break

        DEFAULT_LOGGER.info("Gathering places information".format(str(i)))
        c.execute("SELECT * FROM places_intersection".format(str(i)))
        results = c.fetchall()
        for row in results:
            gid, statefp, placens, name, rid, rname, location, geom = row
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
        roads_info, roads_to_nodes = GraphFactory.__gather_road_data()
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
                        if nodes[i]["id"] == nodes[j]["id"]:
                            continue

                        r.graph.add_edge(nodes[i]["id"], nodes[j]["id"],
                                         id=roads_info[road]["id"],
                                         name=roads_info[road]["name"],
                                         weight=haversine((nodes[i]["lat"], nodes[i]["lon"]),
                                                          (nodes[j]["lat"], nodes[j]["lon"])))
            else:
                DEFAULT_LOGGER.warn("Road ID {0} Not found in Roads Info, cannot make an edge out of it".format(road))

        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(r, pfile, protocol=pickle.HIGHEST_PROTOCOL)

        return r


if __name__ == "__main__":
    gf = GraphFactory()
    gf.construct_graph("/Users/ADINSX/projects/everywhere/graph2.pickle")