#!/usr/bin/python3

import argparse
from src.util.data_util import retrieve_all_census_data
from src.util.data_util import import_data_to_db
from src.dao.graph import RoadGraph


def main():
    parser = argparse.ArgumentParser(description='Main entry point, start up the web server or run utilities')

    def import_data_wrapper():
        import_data_to_db([int(x) for x in args.fips])

    def create_graph():
        RoadGraph.construct_graph(args.graph_name)

    # TODO: Actually implement the web service :p
    def run_webapp():
        pass

    command_help_text = """The desired command:
    download - retrieves data files from the census FTP server
    import - loads the data into the PostGIS database, uses the optional argument fips
    create_graph - creates the road graph data structure, uses the optional argument graph_name
    run - runs the web application, uses the optional argument graph_name
    """

    # I like this way of doing it, http://stackoverflow.com/questions/27529610/call-function-based-on-argparse
    FUNCTION_MAP = {'download': retrieve_all_census_data,
                    'import': import_data_wrapper,
                    'create_graph': create_graph,
                    'run': run_webapp}

    parser.add_argument('command', choices=FUNCTION_MAP.keys(), help=command_help_text)
    parser.add_argument('--fips', nargs="*",
                        help='A list of state FIPS codes to import data for, only used with the "import" command')
    parser.add_argument('--graph_name', nargs=1, default='graph.pickle',
                        help='File name for the graph data structure, only used with the "import" command')

    args = parser.parse_args()
    FUNCTION_MAP[args.command]()

if __name__ == "__main__":
    main()
