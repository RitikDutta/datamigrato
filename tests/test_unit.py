import requests
import pytest
from unit_tests import test_api_response
from unit_tests import test_mongodb_migrator


@pytest.fixture(scope="session")
def client_url(pytestconfig):
    return pytestconfig.getoption("client_url")


def test_api():
    url = 'http://localhost:8080/api/v1/healthcheck'
    # url = 'https://congenial-space-halibut-p7vr5gpwxqgh7qjx-8080.app.github.dev/api/v1/healthcheck'
    test_api_response.Test(url)

def test_mongo(client_url=client_url):
    #mongo creds
    url = 'https://localhost:8080/api/v1/healthcheck/api/v1/public/randomusers?page=1&limit=10'
    # url = 'https://congenial-space-halibut-p7vr5gpwxqgh7qjx-8080.app.github.dev/api/v1/healthcheck'

    database_name = 'db_pytest'
    collection_name = 'col_pytest'
    client_url = client_url
    cred_file = cred_file

    #cassandra creds
    keyspace_name = "data_migrato_key"
    table_name = "new_table"
    secure_bundle = '/home/codered/mystuff/Packages/secure-connect-cassandra-datamigrato.zip'
    token = '/home/codered/mystuff/Packages/cassandra_datamigrato-token.json'
    test_mongodb_migrator.Test(url=url,
                               database_name=database_name,
                               collection_name=collection_name,
                               client_url=client_url,
                               keyspace_name=keyspace_name,
                               table_name=table_name,
                               primary_key="id",
                               secure_bundle=secure_bundle,
                               token=token
                               )
