from datamigrato.migration.mongodb_migrator import Mongo_migrator

class Datamigrato:
    def __init__(self):
        print("Datamigrato class initialized")

    def populate_mongo(self, url, client_url=None, database_name=None, collection_name=None):
        migrator = Mongo_migrator(client_url=client_url, database_name=database_name, collection_name=collection_name)
        migrator.populate_mongo(url)

    def mongo_to_cassandra(self, primary_key='id', flatten=False, client_url=None, database_name=None, collection_name=None, keyspace_name=None, table_name=None, bundle=None, token=None):
        migrator = Mongo_migrator(client_url=client_url, database_name=database_name, collection_name=collection_name)
        migrator.migrate_to_cassandra(primary_key, flatten=flatten)
        print("="*50)
