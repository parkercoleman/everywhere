import json
import shapely
import networkx.exception
from flask import Blueprint, Response
from src import DEFAULT_LOGGER
from src.model.places_dao import PlacesDAO
from src.model.graph import RoadGraph
graph_endpoints = Blueprint('graph', __name__)


def get_graph():
    try:
        g = RoadGraph.load_graph("graph.pickle")
        return g
    except:
        DEFAULT_LOGGER.error("Could not load graph")


@graph_endpoints.route("/places/<name>", methods=["GET"])
def get_places_from_partial_name(name):
    rl = PlacesDAO.get_city_and_state_from_partial(name)
    return Response(json.dumps(rl, indent=4), mimetype='application/json')


@graph_endpoints.route("/calc_route/from/<int:first_id>/to/<int:second_id>")
def calculate_route(first_id, second_id):
    try:
        rsp = get_graph().shortest_route(first_id, second_id)
    except networkx.exception.NetworkXError as e:
        return "Graph error: " + str(e), 400

    for s in rsp.steps:
        s.trimmed_geom = shapely.wkt.dumps(s.trimmed_geom) if s.trimmed_geom is not None else None

    return Response(json.dumps([x.__dict__ for x in rsp.steps], indent=4), mimetype='application/json')
