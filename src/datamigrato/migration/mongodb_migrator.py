from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD

class Mongo_migrator:
    def __init__(self):
        client_url = "mongodb+srv://nisamfaras2:9JKFV21I5PvAZtgi@cluster0.sm6y67x.mongodb.net/?retryWrites=true&w=majority"
        database_name = 'database_test'
        collection_name = 'collection_test'
        mongo_adapter = MongoDB_CRUD(client_url, database_name, collection_name)
        
        self.common_utils = Common_utils()
        self.mongo_data_list = mongo_adapter.read_all()

    def migrate_to_cassandra(self, primary_key, flatten=False):
        
        #cassandra object
        cassandra_adapter = CassandraCRUD(
            "data_migrato_key",
            "new_table",
            "secure-connect-cassandra-datamigrato.zip", 
            "cassandra_datamigrato-token.json"
        )


        # Ensure the table is created only once
        table_created = False

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
            try:
                cassandra_adapter.create(insert_query, tuple(cassandra_data.values()))
                print("Data inserted successfully")
            except Exception as e:
                print(f"An error occurred during insertion: {e}")

    def migrate_to_firebase(self):
        pass

    def migrate_to_firestore(self):
        pass