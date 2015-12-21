import networkx as nx
from src.model.graph_factory import *


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

        return route


class Route:
    """A route is a series of steps"""
    def __init__(self):
        self.steps = []

class Step:
    """A step represents a state in a route, it has a location (lat, lon), a next edge id/name and a db_id"""
    def __init__(self, db_id, lat, lon, next_edge_id, next_edge_name, city_name = None):
        self.db_id = db_id
        self.lat = lat
        self.lon = lon
        self.next_edge_id = next_edge_id
        self.next_edge_name = next_edge_name
        self.city_name = city_name

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)

if __name__ == "__main__":
    rg = GraphFactory.load_graph("/Users/ADINSX/projects/everywhere/graph.pickle")
    route = rg.shortest_route(15, 291)
    print(str(route.steps[0]))