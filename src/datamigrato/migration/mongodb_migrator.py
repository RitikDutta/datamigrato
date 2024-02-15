from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD
from datamigrato.adapters.firebase_realtime_adapter import FirebaseRealtimeDatabaseCRUD

class Mongo_migrator:
    def __init__(self, database_name, collection_name, client_url=None, cred_file=None):
        self.common_utils = Common_utils()
        try:
            client_url = client_url or self.common_utils.read_creds(cred_file).get('client_url')
            if not client_url:
                raise ValueError("Missing required database parameters")
            self.mongo_adapter = MongoDB_CRUD(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
            self.mongo_data_list = self.mongo_adapter.read_all()

        
        except Exception as e:
            print(e)

    def populate_mongo(self, url):
        self.mongo_data_list = data = self.common_utils.get_users_freeAPI(url)
        self.mongo_adapter.create_many(data)

    def migrate_to_cassandra(self, primary_key, keyspace_name=None, table_name=None, secure_bundle=None, token=None, flatten=False):
        cassandra_adapter = CassandraCRUD(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=token)
        cassandra_adapter.insert_json_data(data = self.mongo_data_list, primary_key=primary_key, flatten=flatten)
    def migrate_to_firebase_realtime(self, refrence_url, root_node, token=None):
        firebase_realtime_adapter = FirebaseRealtimeDatabaseCRUD(refrence_url=refrence_url, root_node=root_node, token=token)

    def migrate_to_firestore(self):
        pass