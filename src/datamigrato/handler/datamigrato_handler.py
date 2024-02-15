from datamigrato.migration.mongodb_migrator import Mongo_migrator

class Datamigrato:
    def __init__(self):
        print("Datamigrato class initialized")

    def populate_mongo(self, url, database_name=None, collection_name=None, client_url=None, cred_file=None):
        migrator = Mongo_migrator(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        migrator.populate_mongo(url)

    def mongo_to_cassandra(self, database_name, collection_name, keyspace_name, table_name, primary_key='id', flatten=False, client_url=None, cred_file=None, secure_bundle=None, token=None):
        migrator = Mongo_migrator(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        migrator.migrate_to_cassandra(primary_key=primary_key, keyspace_name=keyspace_name, table_name=table_name, flatten=flatten, secure_bundle=secure_bundle, token=token)
        print("="*50)
