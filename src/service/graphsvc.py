import json
import networkx.exception
from flask import Blueprint, Response
from src.util.nocache import nocache
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
    Goes through the steps list and looks for back to back steps with the same name.
    These steps will be merged.
    :return: a list of dicts that represent steps
    """
    # De-duped steps
    dds = []
    current_group = []
    current_road = ""

    def merge_group(cg):
        if len(cg) == 0:
            raise Exception("merge_group called on an empty list, nothing to merge")
        lat = cg[0].start_lat
        lon = cg[0].start_lon
        name = cg[0].name

        distance_meters = 0
        for s in cg:
            distance_meters += s.distance_meters

        return {
            'lat': lat,
            'lon': lon,
            'next_edge_name': name,
            'distance': {
                'val': '{0:.2f}'.format(distance_meters * 0.000621371)
                    if distance_meters >= 1610
                    else '{0:.2f}'.format(distance_meters * 3.2808388799999997),
                'unit': 'miles' if distance_meters >= 1610 else 'feet'
            },
            'steps': [x.step_id for x in cg]
        }

    for step in steps:
        if current_road != step.name:
            # If we've changed roads, merge all of the previous groups
            if len(current_group) != 0:
                dds.append(merge_group(current_group))
            current_group.clear()
            current_group.append(step)
        else:
            # If we're on the same road, keep adding to the current group
            current_group.append(step)

        current_road = step.name

    return dds


@graph_endpoints.route("/places/<name>", methods=["GET"])
@nocache
def get_places_from_partial_name(name):
    rl = PlacesDAO.get_city_and_state_from_partial(name)
    return Response(json.dumps(rl, indent=4), mimetype='application/json')


@graph_endpoints.route("/calc_route/from/<int:first_id>/to/<int:second_id>")
@nocache
def calculate_route(first_id, second_id):
    try:
        route = get_graph().shortest_route(first_id, second_id)
    except networkx.exception.NetworkXError as e:
        return "Graph error: " + str(e), 400

    route = UserRoutesDAO.insert_and_decorate_route(route)
    steps_rsp = convert_steps_to_json_response(route.steps)

    rsp = {
        'route_id': route.id,
        'steps': steps_rsp
    }

    return Response(json.dumps(rsp, indent=4), mimetype='application/json')
