__author__ = 'pcoleman'
from ftplib import FTP
from zipfile import ZipFile
import os


places_dir = 'data' + os.path.sep + 'places'
roads_dir = 'data' + os.path.sep + 'roads'


def retrieve_data_from_census_ftp(ftp_dir, output_dir):
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
    for file in os.listdir(data_dir):
        if not file.endswith(".zip"):
            continue

        full_name = data_dir + os.path.sep + file
        with ZipFile(full_name) as current_zip:
            current_zip.extractall(data_dir)

        os.remove(full_name)


def retrieve_all_census_data():

    for data_sets in [('/geo/tiger/TIGER2014/PRISECROADS/', roads_dir),
                      ('/geo/tiger/TIGER2014/PLACE/', places_dir)]:

        # retrieve_data_from_census_ftp('/geo/tiger/TIGER2014/PRISECROADS/', 'data/roads')
        # retrieve_data_from_census_ftp('/geo/tiger/TIGER2014/PLACE/', 'data/places')
        extract_all_to_current_dir(data_sets[1])

