import pickle
import os
import uuid
import math
import shapely.wkt
import multiprocessing
import time
from shapely.geometry import Point, LineString
from collections import defaultdict
from main import DEFAULT_LOGGER
from main.model import get_connection, get_node_name_from_location
from main.model.graph import RoadGraph
from main.model.roads_dao import RoadsDAO
from main.config.config import graph_factory_config


def calc_geom(work_array, percent_complete, result_queue):
    """
    This function is used to calculate the geometry between a position and destination, given that they are both on
    an edge.  It is used by the Graph Creation process, its outside the GraphFactory class so it can be serialized and
    run in parallel.

    it takes an array of work (since its run in a process we want to limit the amount of serialization), each entry of
    the work array is a hash map, something like:
        work_unit = {
            "id": id,
            "p1": Point(n1['lon'], n1['lat']),
            "road": road,
            "p2": Point(n2['lon'], n2['lat']),
            "geom": roads_tbl[str(road)]["geom"]
        }
    :param work_array:
    :param percent_complete:
    :param result_queue:
    :return:
    """

    results_map = {}

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

    processed = 0
    for w in work_array:
        try:
            wid, position, edge_id, destination, geom = [w["id"], w["p1"], w["road"], w["p2"], w["geom"]]

            for i in range(0, len(geom)):
                line = geom[i]
                if line.intersects(position) and line.intersects(destination):
                    op = get_line_index(line, position, (0, len(line.coords)-1))
                    dp = get_line_index(line, destination, (0, len(line.coords)-1))

                    begin, end = (op, dp) if op < dp else (dp, op)

                    if begin == end:
                        results_map[wid] = None
                        break

                    results_map[wid] = LineString(line.coords[begin:end+1])
            processed += 1
            percent_complete.value = processed / len(work_array) * 100.0

        except Exception as e:
            print("There was a problem ", e)
            continue

    result_queue.put(results_map)
    percent_complete.value = 100
    return None


class GraphFactory:
    @staticmethod
    def __gather_road_data():
        if os.path.exists("roads_info.pickle") and os.path.exists("roads_to_nodes.pickle"):
            DEFAULT_LOGGER.info("roads_to_nodes.pickle and roads_info.pickle were found, "
                                "loading data from these two files")
            return RoadGraph.load_graph("roads_info.pickle"), RoadGraph.load_graph("roads_to_nodes.pickle")

        roads_info = {}
        roads_to_nodes = defaultdict(list)
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
            gid, name, rid, location = row

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
    def construct_graph(pickle_file_name):
        roads_info, roads_to_nodes = GraphFactory.__gather_road_data()
        # Now we start actually building the graph.  If a road has multiple nodes attached to it,
        # that means those nodes are connected.  We will use the road's length as the weight of that connection.
        # This isn't always strictly correct, but it should be close enough.

        r = RoadGraph()
        graph = r.graph
        roads_tbl = RoadsDAO.get_road_hashmap()

        def calculate_weight(road_edge, geom_subset):
            if geom_subset is None:
                return None

            # If this happens to be an interstate, the road is effectively 5 times longer.
            weight_modifier = 1 if roads_tbl[str(road_edge)]["type"] != "I" else 5
            return geom_subset.length * weight_modifier

        graph_edges = []
        worker_count = graph_factory_config["number_of_processors"]
        work_buckets = []
        worker_status = []
        for i in range(0, worker_count):
            work_buckets.append([])
            worker_status.append(multiprocessing.Value('d', 0.0))
        id_to_geom = {}

        nodes_processed = 0
        for road in roads_to_nodes:
            for i in range(0, len(roads_to_nodes[road])-1):
                for j in range(i+1, len(roads_to_nodes[road])):
                    id = str(uuid.uuid4())
                    assigned_worker = nodes_processed % worker_count

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

                    work_unit = {
                        "id": id,
                        "p1": Point(n1['lon'], n1['lat']),
                        "road": road,
                        "p2": Point(n2['lon'], n2['lat']),
                        "geom": roads_tbl[str(road)]["geom"]
                    }
                    work_buckets[assigned_worker].append(work_unit)

                    graph_edges.append({
                        "n1_name": n1_name,
                        "n2_name": n2_name,
                        "id": id,
                        "db_id": road,
                        "name": roads_info[road]["name"]
                    })

            print('\rConstructing Graph (Creating work units): {0:.2f}%'
                  .format(nodes_processed/len(roads_to_nodes) * 100), end="")
            nodes_processed += 1

        print("")
        for i in range(0, len(work_buckets)):
            print("Work bucket " + str(i) + " Size " + str(len(work_buckets[i])))

        print("Total Graph Edges to be calculated: " + str(len(graph_edges)))

        # Lets start the work processors
        answer_queues = [multiprocessing.SimpleQueue() for i in range(0, worker_count)]
        processors = [multiprocessing.Process(target=calc_geom,
                                              args=(work_buckets[i], worker_status[i], answer_queues[i],))
                      for i in range(0, worker_count)]

        for p in processors:
            p.start()

        while any(s.value != 100 for s in worker_status):
            time.sleep(10)
            output_str = '\rComputing Geometries '
            for i in range(0, len(worker_status)):
                output_str += 'p' + str(i) + " {0:.2f}%\t".format(worker_status[i].value)
            print(output_str, end="")

        for aq in answer_queues:
            while not aq.empty():
                id_to_geom.update(aq.get())

        for p in processors:
            p.join()

        # input("Connect your debugger now!")

        print("")
        nodes_processed = 0
        missing_ids = 0
        for g in graph_edges:
            if g["id"] not in id_to_geom:
                missing_ids += 1
                continue

            geom = id_to_geom[g["id"]]
            r.graph.add_edge(
                    g["n1_name"],
                    g["n2_name"],
                    id=g["id"],
                    db_id=g["db_id"],
                    name=g["name"],
                    weight=calculate_weight(g["db_id"], geom),
                    geom=geom)

            if nodes_processed % 1000 == 0:
                print('\rConstructing Graph (Adding Edges): {0:.2f}%'
                      .format(nodes_processed/len(graph_edges) * 100), end="")

            nodes_processed += 1

        print("Missing " + str(missing_ids))
        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(r, pfile, protocol=pickle.HIGHEST_PROTOCOL)

        return r

if __name__ == "__main__":
    GraphFactory.construct_graph("graph2.pickle")