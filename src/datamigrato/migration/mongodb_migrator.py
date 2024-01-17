from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD

class Mongo_migrator:
    def __init__(self, cred_file=None, client_url=None, database_name=None, collection_name=None):
        self.common_utils = Common_utils()
        try:
            client_url = client_url
            database_name = database_name
            collection_name = collection_name

            if not all([client_url, database_name, collection_name]):
                client_url, database_name, collection_name = self.common_utils.read_credentials(cred_file or 'mongo_creds.yaml')

            if not all([client_url, database_name, collection_name]):
                raise ValueError("Missing required database parameters")
        except Exception as e:
            print(e)

        
        self.mongo_adapter = MongoDB_CRUD(client_url, database_name, collection_name)
        if self.mongo_adapter.is_connected:
            self.mongo_data_list = self.mongo_adapter.read_all()

    def migrate_to_cassandra(self, primary_key, flatten=False):
        
        #cassandra object
        if self.mongo_adapter.is_connected:
            cassandra_adapter = CassandraCRUD(
                "data_migrato_key",
                "new_table",
                "secure-connect-cassandra-datamigrato.zip", 
                "cassandra_datamigrato-token.json"
            )
        else:
            print("Skipping cassandra connection")


        # Ensure the table is created only once
        table_created = False
        try:
            for mongodb_data in self.mongo_data_list:
                # Convert ObjectId to string (or handle it as per your requirement)
                if '_id' in mongodb_data:
                    mongodb_data['_id'] = str(mongodb_data['_id'])

                # Flatten the data if required
                if flatten:
                    mongodb_data = self.common_utils.flatten_data(mongodb_data)

                if primary_key not in mongodb_data:
                    raise ValueError("Primary key not found in the provided data")

                # Create dynamic table if not already created
                if not table_created:
                    cassandra_adapter.create_dynamic_table(mongodb_data.keys(), primary_key)
                    table_created = True

                # Prepare data for insertion
                cassandra_data = {k: str(v) for k, v in mongodb_data.items()}

                # Construct the query
                column_names = ', '.join([f'"{k}"' for k in cassandra_data.keys()])
                placeholders = ', '.join(['%s' for _ in cassandra_data])
                insert_query = f"INSERT INTO {cassandra_adapter.keyspace_name}.{cassandra_adapter.table_name} ({column_names}) VALUES ({placeholders})"
                cassandra_adapter.create(insert_query, tuple(cassandra_data.values()))
        except TypeError as e:
            print(f"Error on reading tables {e}")
        except AttributeError as e:
            print(f"list object not created")

    def migrate_to_firebase(self):
        pass

    def migrate_to_firestore(self):
        pass