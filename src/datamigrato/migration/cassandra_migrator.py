from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD

class Cassandra_migrator:
    def __init__(self, parameter_file=None, keyspace_name=None, table_name=None, secure_bundle=None, token=None):
        self.common_utils = Common_utils()
        try:
            keyspace_name=keyspace_name
            table_name=table_name
            secure_bundle=secure_bundle
        
            if not all([keyspace_name, table_name]):
                parameters = self.common_utils.read_parameters(parameter_file)
                keyspace_name = parameters.get('keyspace_name')
                table_name = parameters.get('table_name')

            if not all([keyspace_name, table_name]):
                raise ValueError("Missing required database parameters")
            
            self.cassandra_adapter = CassandraCRUD(keyspace_name, table_name, secure_bundle, token)

            self.cassandra_data_list = self.cassandra_adapter.read_all()

        except Exception as e:
            print(e)

    def populate_cassandra(self, url, flatten=False):
        self.cassandra_data_list = data = self.common_utils.get_users_freeAPI(url)
        self.cassandra_adapter.insert_json_data(data=data, primary_key='id', flatten=flatten)

    def migrate_to_mongo(self, client_url=None, database_name=None, collection_name=None):


        mongo_adapter = MongoDB_CRUD(client_url=client_url, database_name=database_name, collection_name=collection_name)
        mongo_adapter.create_many(data_list=self.cassandra_data_list)
    
    def migrate_to_firebase(self):
        pass

    def migrate_to_firestore(self):
        pass