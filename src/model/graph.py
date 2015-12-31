import networkx as nx
from src.model.graph_factory import *
from src.model.roads_dao import RoadsDAO
from src.model.places_dao import PlacesDAO
from shapely.geometry import Point, LineString
from haversine import haversine


class RoadGraph:
    """The RoadGraph class is meant to encapsualte the raw pathfinding logic.
    This class builds and encapuslates the road graph, and has methods
    to calculate routes within the graph."""

    def __init__(self, graph_file=None):
        if graph_file is None:
            self.graph = nx.Graph()
        else:
            self.graph = GraphFactory.load_graph(graph_file).graph

    def shortest_route(self, source_id, target_id):
        path = nx.shortest_path(self.graph, source_id, target_id)
        route = Route()

        for i in range(0, len(path)-1):
            n = self.graph.node[path[i]]
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
        return route


class Route:
    """A route is a series of steps"""
    def __init__(self):
        self.steps = []

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)

    def calculate_geom(self):
        """
        For each step in our steps list, pull the road geometry from our database
        and trim to the area we actually drive on.
        :return:
        """
        current_position = None
        for step in self.steps:
            if step.city_name is not None:
                if current_position is None:
                    # This must be our starting city
                    current_position = PlacesDAO.get_place_centroid(step.db_id)
            else:
                # next_id is the road we're going to
                next_id = step.next_edge_id

                # which means that whichever road that isn't next_id is the road we're on
                cur_id = step.db_id.split('_')[0] if step.db_id.split('_')[0] != next_id else step.db_id.split('_')[1]

                cur_geom = RoadsDAO.get_road_geom(cur_id)
                next_geom = RoadsDAO.get_road_geom(next_id)

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

                step.trimmed_geom = LineString(cur_geom[intersection_index['line_index']].coords[begin:end])


class Step:
    """A step represents a state in a route, it has a location (lat, lon), a next edge id/name and a db_id"""
    def __init__(self, db_id, lat, lon, next_edge_id, next_edge_name, city_name = None):
        self.db_id = db_id
        self.lat = lat
        self.lon = lon
        self.next_edge_id = next_edge_id
        self.next_edge_name = next_edge_name
        self.city_name = city_name
        self.trimmed_geom = None

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)

if __name__ == "__main__":
    rg = GraphFactory.load_graph("/Users/ADINSX/projects/everywhere/graph2.pickle")
    route = rg.shortest_route(15, 291)
    print(route)