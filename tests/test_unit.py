import requests
from unit_tests import test_api_response
from datamigrato import test_py

def test_api():
    test_api_response.Test()


def test_module():
    test_py.test_abc()
