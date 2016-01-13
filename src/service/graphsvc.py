import json
import shapely
import networkx.exception
from flask import Blueprint, Response
from src import DEFAULT_LOGGER
from src.model.places_dao import PlacesDAO
from src.model.graph import RoadGraph, Step
from src.model.user_routes_dao import UserRoutesDAO
graph_endpoints = Blueprint('graph', __name__)


def get_graph():
    try:
        g = RoadGraph.load_graph("graph.pickle")
        return g
    except:
        DEFAULT_LOGGER.error("Could not load graph")


def convert_steps_to_json_response(steps):
    """
    Goes through the steps list and looks for back to back steps with the same next_edge name.
    These steps will be merged.  Also removes next_edge_geom from step object
    :return: a list of dicts that represent steps
    """
    # De-duped steps
    dds = []
    current_group = []
    current_road = ""

    def merge_group(cg):
        if len(cg) == 0:
            raise Exception("merge_group called on an empty list, nothing to merge")
        lat = cg[0].lat
        lon = cg[0].lon
        next_edge_id = cg[-1].next_edge_id
        next_edge_name = cg[-1].next_edge_name

        return {
            'lat': lat,
            'lon': lon,
            'next_edge_id': next_edge_id,
            'next_edge_name': next_edge_name,
            'steps': [x.step_id for x in cg]
        }

    for step in steps:
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

    return dds


@graph_endpoints.route("/places/<name>", methods=["GET"])
def get_places_from_partial_name(name):
    rl = PlacesDAO.get_city_and_state_from_partial(name)
    return Response(json.dumps(rl, indent=4), mimetype='application/json')


@graph_endpoints.route("/calc_route/from/<int:first_id>/to/<int:second_id>")
def calculate_route(first_id, second_id):
    try:
        route = get_graph().shortest_route(first_id, second_id)
    except networkx.exception.NetworkXError as e:
        return "Graph error: " + str(e), 400

    route_id = UserRoutesDAO.insert_route(route)
    steps_rsp = convert_steps_to_json_response(route.steps)

    rsp = {
        'route_id': route_id,
        'steps': steps_rsp
    }

    return Response(json.dumps(rsp, indent=4), mimetype='application/json')
