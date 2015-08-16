__author__ = 'pcoleman'
from ftplib import FTP
from zipfile import ZipFile
import os
from src import DEFAULT_LOGGER

from src.dao.db_util import create_tables
from src.dao.db_util import execute_import_statements
from src.dao.db_util import vacuum_full

PLACES_DIR = 'data' + os.path.sep + 'places'
ROADS_DIR = 'data' + os.path.sep + 'roads'


def retrieve_data_from_census_ftp(ftp_dir, output_dir):
    '''
    Connects to the census FTP server (ftp2.census.gov) and downloads the specified directory to the output directory
    :param ftp_dir: the path to the desired data on the census server
    :param output_dir: the output directory the data will be downloaded to
    :return:
    '''
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    f = FTP('ftp2.census.gov')
    f.login()
    f.cwd(ftp_dir)
    files = []

    def file_line_handler(line):
        files.append(line)

    def update_progress(progress):
        print('\rRetrieving File {0}: {1:.2f}%'.format(file, progress), end="")

    def handle_block(block):
        nonlocal bytes_read
        bytes_read += len(block)
        update_progress((bytes_read / file_size) * 100)
        ofile.write(block)

    f.retrlines('NLST', file_line_handler)

    for file in files:
        file_size = f.size(file)
        bytes_read = 0
        with open('%s/%s' % (output_dir, file), 'wb') as ofile:
            f.retrbinary('RETR %s' % file, callback=handle_block)
            print()

    f.quit()


def extract_all_to_current_dir(data_dir):
    '''
    Extracts all zipped files in the data_dir.  Once extracted, the zipped file will be deleted
    :param data_dir:
    :return:
    '''
    for file in os.listdir(data_dir):
        if not file.endswith(".zip"):
            continue

        full_name = data_dir + os.path.sep + file
        with ZipFile(full_name) as current_zip:
            current_zip.extractall(data_dir)

        os.remove(full_name)


def retrieve_all_census_data():
    for data_sets in [('/geo/tiger/TIGER2014/PRISECROADS/', ROADS_DIR),
                      ('/geo/tiger/TIGER2014/PLACE/', PLACES_DIR)]:

        retrieve_data_from_census_ftp(data_sets[0], data_sets[1])
        extract_all_to_current_dir(data_sets[1])


def import_data_to_db(fips=[]):
    '''
    Imports shape file data into the database, requires shp2pgsql to be avialable on the path (it is called via os.popen)
    :param fips: An optional list of state fips codes (integers) to import.
    :return:
    '''
    create_tables()
    for data_dir in (PLACES_DIR, ROADS_DIR):
        for f in os.listdir(data_dir):
            if f.endswith(".shp"):
                # If we don't have a fips list specified,
                # OR our file name contains one of the fips codes we care about...
                if fips == [] or True in ["_" + str(x) + "_" in f for x in fips]:
                    command = "shp2pgsql -s 4269 -a -W latin1 {0} public.{1}"\
                        .format(data_dir + os.path.sep + f,
                                data_dir.split(os.path.sep)[-1])
                    DEFAULT_LOGGER.info("Running " + command)
                    import_lines = os.popen(command).readlines()
                    DEFAULT_LOGGER.info("Importing {0} values into database".format(str(len(import_lines))))
                    execute_import_statements(import_lines)

    vacuum_full()


if __name__ == "__main__":
    import_data_to_db(fips=[22])

