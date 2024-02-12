from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD
from datamigrato.adapters.firebase_realtime_adapter import FirebaseRealtimeDatabaseCRUD

class Firebase_realtime_migrator:
    def __init__(self, refrence_url, root_node, token=None):
        self.common_utils = Common_utils()
        try:
            self.firebase_realtime_adapter = FirebaseRealtimeDatabaseCRUD(refrence_url, root_node, token)
            self.firebase_realtime_data_list = self.firebase_realtime_adapter.read_all()
        except Exception as e:
            print(e)

    def populate_firebase_realtime(self, url):
        self.firebase_realtime_data_list = data = self.common_utils.get_users_freeAPI(url)
        self.firebase_realtime_adapter.insert_json_data(data)
        
    def migrate_to_mongo(self, client_url=None, database_name=None, collection_name=None):        
        mongo_adapter = MongoDB_CRUD(client_url=client_url, database_name=database_name, collection_name=collection_name)
        mongo_adapter.create_many(data_list=self.firebase_realtime_data_list)        

    def migrate_to_cassandra(self, primary_key, keyspace_name=None, table_name=None, secure_bundle=None, token=None, flatten=False):
        cassandra_adapter = CassandraCRUD(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=token)
        cassandra_adapter.insert_json_data(data = self.firebase_realtime_data_list, primary_key=primary_key, flatten=flatten)
