from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD
from datamigrato.adapters.firebase_realtime_adapter import FirebaseRealtimeDatabaseCRUD

class Firebase_realtime_migrator:
    """Handles data migration from Firebase Realtime Database to various databases."""

    def __init__(self, refrence_url, root_node, token=None):
        self.common_utils = Common_utils()
        try:
            self.firebase_realtime_adapter = FirebaseRealtimeDatabaseCRUD(refrence_url, root_node, token)
            self.firebase_realtime_data_list = self.firebase_realtime_adapter.read_all()
        except Exception as e:
            print(e)

    def populate_firebase_realtime(self, url, group_by_field=None):
        """Fetch data from a URL and insert it into Firebase Realtime Database."""

        self.firebase_realtime_data_list = data = self.common_utils.get_users_freeAPI(url)
        self.firebase_realtime_adapter.insert_json_data(data, group_by_field=group_by_field)
        
    def migrate_to_mongo(self, database_name, collection_name, client_url=None, cred_file=None):        
        """Migrate data from Firebase Realtime Database to MongoDB."""

        mongo_adapter = MongoDB_CRUD(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        mongo_adapter.create_many(data_list=self.firebase_realtime_data_list)        

    def migrate_to_cassandra(self, keyspace_name, table_name, primary_key, secure_bundle=None, token=None, flatten=False):
        """Migrate data from Firebase Realtime Database to Cassandra."""
        cassandra_adapter = CassandraCRUD(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=token)
        cassandra_adapter.insert_json_data(data = self.firebase_realtime_data_list, primary_key=primary_key, flatten=flatten)
