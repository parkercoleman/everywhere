import pickle
import os
import uuid
import math
import shapely.wkt
from shapely.geometry import Point, LineString
from collections import defaultdict
from src import DEFAULT_LOGGER
from src.model import get_connection, get_node_name_from_location
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
                        "CREATE INDEX ON gis.roads_intersection USING GIST (geom)"
                        "VACUUM FULL",
                        max_len_query,
                        "CREATE INDEX ON gis.temp_max_length(gid)",
                        "CREATE INDEX ON gis.temp_max_length(max_length)",
                        "VACUUM FULL",
                        places_intersection_query,
                        "VACUUM FULL"]

            DEFAULT_LOGGER.info("Creating places and roads intersection tables, this can take hours on a slow machine")
            e_conn = get_connection()
            e_conn.autocommit = True
            try:
                for command in commands:
                    DEFAULT_LOGGER.info("Executing: " + command)
                    e_conn.cursor().execute(command)
            finally:
                e_conn.close()

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

                p = shapely.wkt.loads(intersection_point)

                node = {"id": get_node_name_from_location(p),
                        "lat": p.y,
                        "lon": p.x}
                roads_to_nodes[r1id].append(node)
                roads_to_nodes[r2id].append(node)

            if not have_results:
                break

        DEFAULT_LOGGER.info("Gathering places information".format(str(i)))
        c.execute("SELECT * FROM gis.places_intersection".format(str(i)))
        results = c.fetchall()
        for row in results:
            gid, statefp, placens, name, rid, rname, location, geom = row

            p = shapely.wkt.loads(location)
            # Create a city node, similar to an intersection node, but with different tags (namely a "city_name")
            roads_to_nodes[int(rid)].append({"id": gid,
                                             "city_name": name,
                                             "lat": p.y,
                                             "lon": p.x})

        # Write out these results to disk
        for ftuple in (("roads_info.pickle", roads_info), ("roads_to_nodes.pickle", roads_to_nodes)):
            with open(ftuple[0], 'wb') as pfile:
                pickle.dump(ftuple[1], pfile, protocol=pickle.HIGHEST_PROTOCOL)

        return roads_info, roads_to_nodes

    @staticmethod
    def __calc_geom(position, edge_id, destination, roads_tbl):
        geom = roads_tbl[str(edge_id)]

        def get_line_index(line, position, index):
            if index[0] == index[1]:
                return index[0]

            if index[1] - index[0] == 1:
                p = index[0] if Point(line.coords[index[0]]).distance(position) < Point(line.coords[index[1]]).distance(position) else index[1]
                return p

            pivot_point = math.floor((index[1] - index[0])/2) + index[0]
            i1 = (index[0], pivot_point)
            i2 = (pivot_point+1, index[1])

            if i1[0] != i1[1] and LineString(line.coords[i1[0]:i1[1]+1]).intersects(position):
                return get_line_index(line, position, i1)
            else:
                return get_line_index(line, position, i2)

        for i in range(0, len(geom)):
            line = geom[i]
            if line.intersects(position) and line.intersects(destination):
                op = get_line_index(line, position, (0, len(line.coords)-1))
                dp = get_line_index(line, destination, (0, len(line.coords)-1))

                begin, end = (op, dp) if op < dp else (dp, op)

                if begin == end:
                    return None

                return LineString(line.coords[begin:end+1])

    @staticmethod
    def construct_graph(pickle_file_name):
        roads_info, roads_to_nodes = GraphFactory.__gather_road_data()
        # Now we start actually building the graph.  If a road has multiple nodes attached to it,
        # that means those nodes are connected.  We will use the road's length as the weight of that connection.
        # This isn't always strictly correct, but it should be close enough.

        r = RoadGraph()
        graph = r.graph
        nodes_processed = 0

        roads_tbl = RoadsDAO.get_road_hashmap()

        for road in roads_to_nodes:
            for i in range(0, len(roads_to_nodes[road])-1):
                for j in range(i+1, len(roads_to_nodes[road])):
                    n1 = roads_to_nodes[road][i]
                    n2 = roads_to_nodes[road][j]
                    n1_name = str(n1['lat']) + "," + str(n1['lon']) if "city_name" not in n1 else n1["id"]
                    n2_name = str(n2['lat']) + "," + str(n2['lon']) if "city_name" not in n2 else n2["id"]

                    if n1_name == n2_name:
                        continue

                    if n1_name not in graph:
                        graph.add_node(n1_name, attr_dict=n1)
                    if n2_name not in graph:
                        graph.add_node(n2_name, attr_dict=n2)

                    geom = GraphFactory.__calc_geom(Point(n1['lon'], n1['lat']), road, Point(n2['lon'], n2['lat']), roads_tbl)

                    r.graph.add_edge(n1_name, n2_name,
                                     id=str(uuid.uuid4()),
                                     db_id=road,
                                     name=roads_info[road]["name"],
                                     weight=geom.length if geom is not None else None,
                                     geom=geom)
                                     # weight=haversine((n1['lat'], n1['lon']), (n2['lat'], n2['lon'])))

            print('\rConstructing Graph: {0:.2f}%'.format(nodes_processed/len(roads_to_nodes) * 100), end="")
            nodes_processed += 1

        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(r, pfile, protocol=pickle.HIGHEST_PROTOCOL)

        return r

if __name__ == "__main__":
    GraphFactory.construct_graph("graph2.pickle")