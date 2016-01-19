__author__ = 'pcoleman'

from main.model.graph import *


def test_create_node_from_db_results():
    r = Node.create_node_from_db_results(123, 456, "POINT(90 30)")
    assert r.lat == 30
    assert r.lon == 90
    assert r.id == "123_456"
