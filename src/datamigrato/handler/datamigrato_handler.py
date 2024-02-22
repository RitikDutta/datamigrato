from datamigrato.migration.mongodb_migrator import Mongo_migrator
from datamigrato.migration.cassandra_migrator import Cassandra_migrator
from datamigrato.migration.firebase_realtime_migrator import Firebase_realtime_migrator

class Datamigrato:
    def __init__(self):
        print("Datamigrato class initialized")

    # MONGO
    def populate_mongo(self, url, database_name, collection_name, client_url=None, cred_file=None):
        print("---Populating---")
        migrator = Mongo_migrator(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        migrator.populate_mongo(url)

    def mongo_to_cassandra(self, database_name, collection_name, keyspace_name, table_name, client_url=None, cred_file=None, primary_key='id', flatten=False, secure_bundle=None, cassandra_token=None):
        print("---Migrating---")
        migrator = Mongo_migrator(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        migrator.migrate_to_cassandra(primary_key=primary_key, keyspace_name=keyspace_name, table_name=table_name, flatten=flatten, secure_bundle=secure_bundle, token=cassandra_token)

    def mongo_to_firebase(self, database_name, collection_name, refrence_url, root_node, client_url=None, cred_file=None, group_by=None, firebase_realtime_token=None):
        print("---Migrating---")
        migrator = Mongo_migrator(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        migrator.migrate_to_firebase_realtime(refrence_url=refrence_url, root_node=root_node, group_by=group_by, token=firebase_realtime_token)

    # CASSANDRA
    def populate_cassandra(self, url, keyspace_name, table_name, primary_key, flatten=False, secure_bundle=None, cassandra_token=None):
        print("---Populating---")
        migrator = Cassandra_migrator(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=cassandra_token)
        migrator.populate_cassandra(url=url, primary_key=primary_key, flatten=flatten)

    def cassandra_to_mongo(self, keyspace_name, table_name, database_name, collection_name, secure_bundle=None, cassandra_token=None, client_url=None, cred_file=None):
        print("---Migrating---")
        migrator = Cassandra_migrator(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=cassandra_token)
        migrator.migrate_to_mongo(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)

    def cassandra_to_firebase_realtime(self, keyspace_name, table_name, refrence_url, root_node, secure_bundle=None, cassandra_token=None, group_by=None, firebase_realtime_token=None):
        print("---Migrating---")
        migrator = Cassandra_migrator(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=cassandra_token)
        migrator.migrate_to_firebase_realtime(refrence_url=refrence_url, root_node=root_node, token=firebase_realtime_token)

    # FIREBASE_REALTIME
    def populate_firebase_realtime(self, url, refrence_url, root_node, group_by_field=None, firebase_realtime_token=None):
        print("---Populating---")
        migrator = Firebase_realtime_migrator(refrence_url, root_node, token=firebase_realtime_token)
        migrator.populate_firebase_realtime(url=url, group_by_field=group_by_field)
    
    def firebase_realtime_to_mongo(self, refrence_url, root_node, database_name, collection_name, firebase_realtime_token=None, client_url=None, cred_file=None):
        print("---Migrating---")
        migrator = Firebase_realtime_migrator(refrence_url, root_node, token=firebase_realtime_token)
        migrator.migrate_to_mongo(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)

    def firebase_realtime_to_cassandra(self, refrence_url, root_node, keyspace_name, table_name, firebase_realtime_token=None, primary_key='id', flatten=False, secure_bundle=None, cassandra_token=None):
        print("---Migrating---")
        migrator = Firebase_realtime_migrator(refrence_url, root_node, token=firebase_realtime_token)
        migrator.migrate_to_cassandra(primary_key=primary_key, keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=cassandra_token, flatten=flatten)

