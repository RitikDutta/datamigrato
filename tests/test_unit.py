import requests
from unit_tests import test_api_response
from unit_tests import test_mongodb_migrator

def test_api():
    url = 'http://localhost:8080/api/v1/healthcheck'
    # url = 'https://literate-guide-5v6q74x5vw5c4q44-8080.app.github.dev/api/v1/healthcheck'
    test_api_response.Test(url)

def test_mongo():
    test_mongodb_migrator.Test(bundle="/home/runner/work/datamigrato/datamigrato/secure-connect-cassandra-datamigrato.zip", token="/home/runner/work/datamigrato/datamigrato/cassandra_datamigrato-token.json")

test_mongo()