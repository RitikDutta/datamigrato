from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD
from datamigrato.adapters.firebase_realtime_adapter import FirebaseRealtimeDatabaseCRUD

class Mongo_migrator:
    def __init__(self, parameter_file=None, client_url=None, database_name=None, collection_name=None):
        self.common_utils = Common_utils()
        try:
            client_url = client_url
            database_name = database_name
            collection_name = collection_name

            if not all([client_url, database_name, collection_name]):
                parameters = self.common_utils.read_parameters(parameter_file)
                client_url = parameters.get('client_url')
                database_name = parameters.get('database_name')
                collection_name = parameters.get('collection_name')

            if not all([client_url, database_name, collection_name]):
                raise ValueError("Missing required database parameters")

            self.mongo_adapter = MongoDB_CRUD(client_url, database_name, collection_name)
        
            self.mongo_data_list = self.mongo_adapter.read_all()

        except Exception as e:
            print(e)

    def populate_mongo(self, url):
        self.mongo_data_list = data = self.common_utils.get_users_freeAPI(url)
        self.mongo_adapter.create_many(data)

    def migrate_to_cassandra(self, primary_key, keyspace_name=None, table_name=None, bundle=None, token=None, flatten=False):
        cassandra_adapter = CassandraCRUD(keyspace_name, table_name, bundle, token)
        cassandra_adapter.insert_json_data(data = self.mongo_data_list, primary_key=primary_key, flatten=flatten)

    def migrate_to_firebase_realtime(self, refrence_url, root_node, token=None):
        pass

    def migrate_to_firestore(self):
        pass