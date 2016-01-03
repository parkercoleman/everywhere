import pickle
import os
import networkx as nx
from shapely.geometry import Point, LineString
from shapely.ops import cascaded_union
from collections import defaultdict
from src import DEFAULT_LOGGER
from haversine import haversine
from src.model import get_connection
from src.model.graph import RoadGraph
from src.model.roads_dao import RoadsDAO



class GraphFactory:
    @staticmethod
    def __gather_road_data():
        if os.path.exists("roads_info.pickle") and os.path.exists("roads_to_nodes.pickle"):
            DEFAULT_LOGGER.info("roads_to_nodes.pickle and roads_info.pickle were found, "
                                "loading data from these two files")

            return RoadGraph.load_graph("roads_info.pickle"), RoadGraph.load_graph("roads_to_nodes.pickle")

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
            INTO TABLE gis.roads_intersection
            FROM gis.roads r1
            INNER JOIN gis.roads r2 ON ((ST_Touches(r1.geom, r2.geom) OR ST_Intersects(r1.geom, r2.geom))
                AND GeometryType(ST_Intersection(r1.geom, r2.geom)) = 'POINT'
                AND r1.linearid != r2.linearid
                AND r2.rttyp NOT IN ('I')
                AND NOT ST_Equals(r1.geom, r2.geom))
            WHERE r1.rttyp NOT IN ('I')
        """

        max_len_query = """
            SELECT p.gid, MAX(ST_Length(ST_Intersection(p.geom, r.geom), true)) as max_length
            INTO TABLE gis.temp_max_length
            FROM gis.places p
            INNER JOIN gis.roads r ON ST_Intersects(p.geom, r.geom)
            INNER JOIN gis.roads_intersection ri ON (r.linearid = ri.r1id OR r.linearid = ri.r2id)
            WHERE p.lsad = '25'
            AND r.rttyp NOT IN ('I', 'M')
            GROUP BY p.gid
        """

        places_intersection_query = """
            SELECT p.gid, statefp, placens, name, r.linearid, r.fullname,
                ST_AsText(ST_Centroid(ST_Intersection(p.geom, r.geom))) as location,
                ST_Centroid(ST_Intersection(p.geom, r.geom)) as geom
            INTO TABLE gis.places_intersection
            FROM gis.places p
            INNER JOIN gis.roads r ON ST_Intersects(p.geom, r.geom)
            INNER JOIN gis.temp_max_length ml
                ON (ST_Length(ST_Intersection(p.geom, r.geom), true) = ml.max_length AND p.gid = ml.gid)
            WHERE p.lsad = '25'
            AND r.rttyp NOT IN ('I', 'M')
            """

        roads_info = {}
        roads_to_nodes = defaultdict(list)

        def create_temp_tables():
            commands = ["DROP TABLE IF EXISTS gis.roads_intersection",
                        road_intersection_query,
                        "DROP TABLE IF EXISTS gis.places_intersection",
                        "DROP TABLE IF EXISTS gis.temp_max_length",
                        "CREATE INDEX ON gis.roads_intersection(r1id)",
                        "CREATE INDEX ON gis.roads_intersection(r2id)",
                        "VACUUM FULL",
                        max_len_query,
                        "CREATE INDEX ON gis.temp_max_length(gid)",
                        "CREATE INDEX ON gis.temp_max_length(max_length)",
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
            c.execute("SELECT * FROM gis.roads_intersection ORDER BY r1id, r2id LIMIT 100000 OFFSET {0}".format(str(i)))
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
        c.execute("SELECT * FROM gis.places_intersection".format(str(i)))
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
    def __add_geom_to_edges(road_graph):
        """
        This method adds the trimmed geometry of the roads that connect edges.  It also adds an accurate weight to the edges
        :param RoadGraph:
        :return:
        """

        def calc_geom(position, edge_id, destination, conn):
            """
            Calcuated the trimmed geom for this edge id
            :param position: A Shapely Point object
            :param edge_id:  The ID of the road this edge is on
            :param destination: A Shapely Point object
            :return:
            """
            cur_geom = RoadsDAO.get_road_geom(edge_id, connection=conn)

            intersection_index = {
                "line_index": -1,
                "point_index": -1,
                "distance": 9999999999
            }
            origin_index = {
                "line_index": -1,
                "point_index": -1,
                "distance": 9999999999
            }

            # We need to find the closest points in our MultiLine to the intersection point, and the origin point
            for i in range(0, len(cur_geom)):
                line = cur_geom[i]
                for j in range(0, len(line.coords)):
                    p = line.coords[j]
                    if haversine((destination.y, destination.x), (p[1], p[0])) \
                            < intersection_index['distance']:
                        intersection_index["line_index"] = i
                        intersection_index["point_index"] = j
                        intersection_index["distance"] = haversine((destination.y, destination.x), (p[1], p[0]))

                    if haversine((position.y, position.x), (p[1], p[0])) \
                            < origin_index['distance']:
                        origin_index["line_index"] = i
                        origin_index["point_index"] = j
                        origin_index["distance"] = haversine((position.y, position.x), (p[1], p[0]))

            if intersection_index['line_index'] != origin_index['line_index']:
                DEFAULT_LOGGER.error("Our origin and destination were not on the same line for edge: " + edge_id)
                return None

            begin, end = (intersection_index['point_index'], origin_index['point_index']) \
                if intersection_index['point_index'] < origin_index['point_index'] \
                else (origin_index['point_index'], intersection_index['point_index'])

            if begin == end:
                return None

            return LineString(cur_geom[intersection_index['line_index']].coords[begin:end+1])

        g = road_graph.graph
        conn = get_connection()
        i = 0
        try:
            for cur_node, cur_data in g.nodes_iter(data=True):
                for dest_node in nx.all_neighbors(g, cur_node):
                    if 'geom' not in g[cur_node][dest_node]:
                        dest_data = g.node[dest_node]
                        edge_id = g[cur_node][dest_node]['id']

                        # Ur pos is a pos
                        current_pos = Point(cur_data['lon'], cur_data['lat'])
                        dest_pos = Point(dest_data['lon'], dest_data['lat'])

                        try:
                            g[cur_node][dest_node]['geom'] = calc_geom(current_pos, edge_id, dest_pos, conn)
                        except Exception as e:
                            DEFAULT_LOGGER.warn("Could not create trimmed geometry between nodes: " +
                                                cur_node + " and " + dest_node + " Because: " + str(e))

                i += 1
                if i % 100 == 0:
                    DEFAULT_LOGGER.info("Computed geometries for " + str(i) + " nodes out of " + str(nx.number_of_nodes(g)))



        finally:
            conn.close()


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

        GraphFactory.__add_geom_to_edges(r)

        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(r, pfile, protocol=pickle.HIGHEST_PROTOCOL)

        return r

if __name__ == "__main__":
    GraphFactory.construct_graph("graph.pickle")