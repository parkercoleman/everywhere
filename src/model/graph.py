import networkx as nx
import pickle
import abc
import uuid
from geopy import distance
from src.model.places_dao import PlacesDAO


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
        route_id = str(uuid.uuid4())
        route = Route(route_id,
                      self.graph.node[path[0]]['lat'],
                      self.graph.node[path[0]]['lon'],
                      PlacesDAO.get_place_name_by_id(source_id) + " to " + PlacesDAO.get_place_name_by_id(target_id))

        for i in range(0, len(path)):
            n = self.graph.node[path[i]]
            if i+1 == len(path):
                # this is the final step in the path, so there is no next edge
                route.steps.append(Step(
                    route_id,
                    i,
                    n['lat'],
                    n['lon'],
                    None,
                    None,
                    n['city_name'] if 'city_name' in n else None,
                ))
            else:
                next_road = self.graph.get_edge_data(path[i], path[i+1])
                route.steps.append(Step(
                    route_id,
                    i,
                    n['lat'],
                    n['lon'],
                    next_road['name'],
                    next_road['geom'],
                    n['city_name'] if 'city_name' in n else None
                ))

        return route


class AbstractRoute:
    __metaclass__ = abc.ABCMeta

    def __init__(self, rid, start_lat, start_lon, r_name, geom):
        self.id = rid
        self.start_lat = start_lat
        self.start_lon = start_lon
        self.name = r_name
        self.geom = geom

        # These values will be calculated later
        self.distance_meters = 0
        self.geom_centroid = None
        self.geom_bbox = None


class Step(AbstractRoute):
    def __init__(self, rid, sid, start_lat, start_lon, r_name, geom, city_name=None):
        AbstractRoute.__init__(self, rid, start_lat, start_lon, r_name, geom)
        self.step_id = sid
        self.city_name = city_name
        self.turn = None

    def __str__(self):
        return str(self.__dict__)


class Route(AbstractRoute):
    def __init__(self, rid, start_lat, start_lon, r_name):
        AbstractRoute.__init__(self, rid, start_lat, start_lon, r_name, None)
        self.steps = []

    def __str__(self):
        return str(self.__dict__)
