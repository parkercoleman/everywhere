__author__ = 'pcoleman'

import logging

# TODO: I really don't feel like setting up logging right now,
# just going to put this here for some default stuff, I'll configure it to log to a file later.

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')

DEFAULT_LOGGER = logging.getLogger('')
