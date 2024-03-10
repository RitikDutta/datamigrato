import glob
from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD
from datamigrato.adapters.firebase_realtime_adapter import FirebaseRealtimeDatabaseCRUD

class Mongo_migrator:
    """A class to handle data migration from MongoDB to various databases."""

    def __init__(self, database_name, collection_name, client_url=None, cred_file=None):
        """
        Initialize the migrator with MongoDB collection details.

        Args:
            database_name (str): The name of the MongoDB database.
            collection_name (str): The name of the MongoDB collection.
            client_url (str, optional): The MongoDB connection URL. Defaults to None.
            cred_file (str, optional): Path to the credentials file if client_url is not provided. Defaults to None.
        """

        self.common_utils = Common_utils()
        try:
            client_url = client_url or self.common_utils.read_creds(cred_file).get('client_url')
            if not client_url:
                raise ValueError("Missing required database parameters")
            self.mongo_adapter = MongoDB_CRUD(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
            self.mongo_data_list = self.mongo_adapter.read_all()

        except Exception as e:
            print("Failed to get mongo object", e)

    def populate_mongo(self, url):
        """Fetch data from a URL and insert it into MongoDB."""

        try:
            self.mongo_data_list = data = self.common_utils.get_users_freeAPI(url)
            self.mongo_adapter.create_many(data)
        except Exception as e:
            print(e)

    def migrate_to_cassandra(self, primary_key, keyspace_name, table_name, secure_bundle=None, token=None, flatten=False):
        """Migrate data from MongoDB to Cassandra."""

        try:
            cassandra_adapter = CassandraCRUD(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=token)
            data = self.common_utils.convert_bson_to_json(self.mongo_data_list)
            cassandra_adapter.insert_json_data(data = data, primary_key=primary_key, flatten=flatten)
        except Exception as e:
            print(e)
        except:
            print ("Execution stopped")
    def migrate_to_firebase_realtime(self, refrence_url, root_node, group_by=None, token=None):
        """Migrate data from MongoDB to firebase realtime."""

        try:
            firebase_realtime_adapter = FirebaseRealtimeDatabaseCRUD(refrence_url=refrence_url, root_node=root_node, token=token)
            data = self.common_utils.convert_bson_to_json(self.mongo_data_list)
            firebase_realtime_adapter.insert_json_data(data=data, group_by_field=group_by)
        except Exception as e:
            print(e)
        except:
            print("Execution stopped")
    def migrate_to_firestore(self):
        pass