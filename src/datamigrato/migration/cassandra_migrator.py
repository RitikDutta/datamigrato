from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD

class Cassandra_migrator:
    def __init__(self, keyspace_name, table_name, secure_bundle=None, token=None):
        self.common_utils = Common_utils()
        try:
            self.cassandra_adapter = CassandraCRUD(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=token)
            self.cassandra_data_list = self.cassandra_adapter.read_all()

        except Exception as e:
            print(e)

    def populate_cassandra(self, url, flatten=False):
        self.cassandra_data_list = data = self.common_utils.get_users_freeAPI(url)
        self.cassandra_adapter.insert_json_data(data=data, primary_key='id', flatten=flatten)

    def migrate_to_mongo(self, database_name=None, collection_name=None, client_url=None, cred_file=None):
        mongo_adapter = MongoDB_CRUD(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        mongo_adapter.create_many(data_list=self.cassandra_data_list)
    
    def migrate_to_firebase(self, refrence_url, root_node, token=None):
        pass

    def migrate_to_firestore(self):
        pass