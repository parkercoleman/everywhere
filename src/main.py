__author__ = 'pcoleman'
import argparse
from src.util.data_util import retrieve_all_census_data
from src.util.data_util import import_data_to_db

parser = argparse.ArgumentParser(description='Main entry point, start up the web server or run utilities')


def import_data_wrapper():
    import_data_to_db([int(x) for x in args.fips])


# TODO: Actually implement the web service :p
def run_webapp():
    pass

# I like this way of doing it, http://stackoverflow.com/questions/27529610/call-function-based-on-argparse
FUNCTION_MAP = {'download': retrieve_all_census_data,
                'import': import_data_wrapper,
                'run': run_webapp}

parser.add_argument('command', choices=FUNCTION_MAP.keys())
parser.add_argument('fips', nargs="*",
                    help='A list of state FIPS codes to import data for, only used with the "import" command')

args = parser.parse_args()
FUNCTION_MAP[args.command]()
