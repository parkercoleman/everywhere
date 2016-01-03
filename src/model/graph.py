import networkx as nx
import pickle
from src import DEFAULT_LOGGER
from src.model.roads_dao import RoadsDAO
from src.model.places_dao import PlacesDAO
from shapely.geometry import Point, LineString
from shapely.ops import cascaded_union
from haversine import haversine


class RoadGraph:
    """The RoadGraph class is meant to encapsualte the raw pathfinding logic.
    This class builds and encapuslates the road graph, and has methods
    to calculate routes within the graph."""

    @staticmethod
    def save_graph(self, pickle_file_name):
        with open(pickle_file_name, 'wb') as pfile:
            pickle.dump(self, pfile, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load_graph(pickle_file_name):
        with open(pickle_file_name, 'rb') as pfile:
            return pickle.load(pfile)

    def __init__(self, graph_file=None):
        if graph_file is None:
            self.graph = nx.Graph()
        else:
            self.graph = RoadGraph.load_graph(graph_file).graph

    def shortest_route(self, source_id, target_id):
        path = nx.shortest_path(self.graph, source_id, target_id)
        route = Route()

        for i in range(0, len(path)):
            n = self.graph.node[path[i]]
            if i+1 == len(path):
                route.steps.append(Step(
                    n['id'],
                    n['lat'],
                    n['lon'],
                    None,
                    None,
                    n['city_name'] if 'city_name' in n else None
                ))
            else:
                next_road = self.graph.get_edge_data(path[i], path[i+1])
                route.steps.append(Step(
                    n['id'],
                    n['lat'],
                    n['lon'],
                    next_road['id'],
                    next_road['name'],
                    n['city_name'] if 'city_name' in n else None
                ))

        route.calculate_geom()
        route.dedupe_steps()
        return route


class Route:
    """A route is a series of steps"""
    def __init__(self):
        self.steps = []

    def __str__(self):
        return str(self.__dict__)

    def dedupe_steps(self):
        """
        Goes through the self.steps list and looks for back to back steps with the same next_edge name.
        These steps will be merged.
        :return:
        """
        dds = []
        current_group = []
        current_road = ""

        def merge_group(cg):
            if len(cg) == 0:
                raise Exception("merge_group called on an empty list, nothing to merge")
            lat = cg[0].lat
            lon = cg[0].lon
            db_id = cg[0].db_id
            next_edge_id = [x.next_edge_id for x in cg]
            next_edge_name = cg[-1].next_edge_name
            trimmed_geom = cascaded_union([x.trimmed_geom for x in cg if x.trimmed_geom is not None])

            return Step(db_id, lat, lon, next_edge_id, next_edge_name, city_name=None, trimmed_geom=trimmed_geom)

        for step in self.steps:
            if current_road != step.next_edge_name:
                # If we've changed roads, merge all of the previous groups
                if len(current_group) != 0:
                    dds.append(merge_group(current_group))
                current_group.clear()
                current_group.append(step)
            else:
                # If we're on the same road, keep adding to the current group
                current_group.append(step)

            current_road = step.next_edge_name

        self.steps.clear()
        self.steps = dds

    def calculate_geom(self):
        """
        For each step in our steps list, pull the road geometry from our database
        and trim to the area we actually drive on.
        :return:
        """
        current_position = None
        for step in self.steps:
            if current_position is None:
                # This must be our starting city
                current_position = PlacesDAO.get_place_centroid(step.db_id)
                continue

            if step.next_edge_id is None:
                # We are at the destination, we're done
                continue

            # next_id is the road we're going to
            next_id = step.next_edge_id

            # which means that whichever road that isn't next_id is the road we're on
            cur_id = step.db_id.split('_')[0] if step.db_id.split('_')[0] != next_id else step.db_id.split('_')[1]

            cur_geom = RoadsDAO.get_road_geom(cur_id)

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

            intersection_position = Point(step.lon, step.lat)
            # We need to find the closest points in our MultiLine to the intersection point, and the origin point
            for i in range(0, len(cur_geom)):
                line = cur_geom[i]
                for j in range(0, len(line.coords)):
                    p = line.coords[j]
                    if haversine((intersection_position.y, intersection_position.x), (p[1], p[0])) \
                            < intersection_index['distance']:
                        intersection_index["line_index"] = i
                        intersection_index["point_index"] = j
                        intersection_index["distance"] = haversine((intersection_position.y, intersection_position.x), (p[1], p[0]))

                    if haversine((current_position.y, current_position.x), (p[1], p[0])) \
                            < origin_index['distance']:
                        origin_index["line_index"] = i
                        origin_index["point_index"] = j
                        origin_index["distance"] = haversine((current_position.y, current_position.x), (p[1], p[0]))

            if intersection_index['line_index'] != origin_index['line_index']:
                DEFAULT_LOGGER.error("Our origin and destination were not on the same line for step " + step.db_id)
                continue

            begin, end = (intersection_index['point_index'], origin_index['point_index']) \
                if intersection_index['point_index'] < origin_index['point_index'] \
                else (origin_index['point_index'], intersection_index['point_index'])

            if begin == end:
                continue

            step.trimmed_geom = LineString(cur_geom[intersection_index['line_index']].coords[begin:end+1])


class Step:
    """A step represents a state in a route, it has a location (lat, lon), a next edge id/name and a db_id"""
    def __init__(self, db_id, lat, lon, next_edge_id, next_edge_name, city_name=None, trimmed_geom=None):
        self.db_id = db_id
        self.lat = lat
        self.lon = lon
        self.next_edge_id = next_edge_id
        self.next_edge_name = next_edge_name
        self.city_name = city_name
        self.trimmed_geom = trimmed_geom

    def __str__(self):
        return str(self.__dict__)
