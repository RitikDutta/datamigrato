from unit_tests import test_mongodb_migrator

def test_mongo():
    test_mongodb_migrator.mongo_to_cassandra()
