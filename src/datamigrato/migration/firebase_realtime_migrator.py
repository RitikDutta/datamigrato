from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD
from datamigrato.adapters.firebase_realtime_adapter import FirebaseRealtimeDatabaseCRUD

class Firebase_realtime_migrator:
    def __init__(self, parameter_file=None, refrence_url=None, root_node=None, token=None):
        self.common_utils = Common_utils()
        try:
            refrence_url = refrence_url
            root_node = root_node

            if not all([refrence_url, root_node]):
                parameters = self.common_utils.read_parameters(parameter_file)
                refrence_url = parameters.get('refrence_url')
                root_node = parameters.get('root_node')
            
            if not all([refrence_url, root_node]):
                raise ValueError("Missing required database parameters")

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

    def migrate_to_cassandra(self, primary_key, keyspace_name=None, table_name=None, bundle=None, token=None, flatten=False):
        keyspace_name = keyspace_name or "data_migrato_key"
        table_name = table_name or "new_table"

        #cassandra object
        cassandra_adapter = CassandraCRUD(keyspace_name, table_name, bundle, token)
        cassandra_adapter.insert_json_data(data = self.firebase_realtime_data_list, primary_key=primary_key, flatten=flatten)
