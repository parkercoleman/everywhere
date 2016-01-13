#!/usr/bin/python3

import argparse
from src.util.data_util import retrieve_all_census_data
from src.util.data_util import import_data_to_db
from src.model.graph_factory import GraphFactory
from src import DEFAULT_LOGGER
import src.service.graphsvc


def main():
    parser = argparse.ArgumentParser(description='Main entry point, start up the web server or run utilities')

    def import_data_wrapper():
        import_data_to_db([int(x) for x in args.fips])

    def create_graph():
        GraphFactory.construct_graph(args.graph_name)

    def run_webapp():
        from flask import Flask
        flask_app = Flask(__name__, static_url_path='')
        flask_app.register_blueprint(src.service.graphsvc.graph_endpoints, url_prefix='/graph')
        DEFAULT_LOGGER.info(flask_app.url_map)
        flask_app.run()

    command_help_text = """The desired command:
    download - retrieves data files from the census FTP server
    import - loads the data into the PostGIS database, uses the optional argument fips
    create_graph - creates the road graph data structure, uses the optional argument graph_name
    run - runs the web application, uses the optional argument graph_name
    """

    # I like this way of doing it, http://stackoverflow.com/questions/27529610/call-function-based-on-argparse
    function_map = {'download': retrieve_all_census_data,
                    'import': import_data_wrapper,
                    'create_graph': create_graph,
                    'run': run_webapp}

    parser.add_argument('command', choices=function_map.keys(), help=command_help_text)
    parser.add_argument('--fips', nargs="*",
                        help='A list of state FIPS codes to import data for, only used with the "import" command')
    parser.add_argument('--graph_name', nargs=1, default='graph.pickle',
                        help='File name for the graph data structure, only used with the "import" command')

    args = parser.parse_args()
    function_map[args.command]()

if __name__ == "__main__":
    main()
