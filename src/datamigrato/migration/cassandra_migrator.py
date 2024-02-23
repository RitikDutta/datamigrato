from datamigrato.utils.common_utils import Common_utils
from datamigrato.adapters.mongodb_adapter import MongoDB_CRUD
from datamigrato.adapters.cassandra_adapter import CassandraCRUD
from datamigrato.adapters.firebase_realtime_adapter import FirebaseRealtimeDatabaseCRUD

class Cassandra_migrator:
    """A class to handle data migration from Cassandra to various databases."""

    def __init__(self, keyspace_name, table_name, secure_bundle=None, token=None):
        """
        Initialize a connection to a Cassandra table.

        Args:
            keyspace_name (str): The name of the Cassandra keyspace.
            table_name (str): The name of the Cassandra table.
            secure_bundle (str, optional): Path to the secure bundle for Cassandra connection. Defaults to None.
            token (str, optional): Authentication token for Cassandra connection. Defaults to None.
        """

        self.common_utils = Common_utils()
        try:
            self.cassandra_adapter = CassandraCRUD(keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=token)
            self.cassandra_data_list = self.cassandra_adapter.read_all()

        except Exception as e:
            print(e)
        except:
            print("failed")

    def populate_cassandra(self, url, primary_key, flatten=False):
        """Fetch data from a URL and insert it into Cassandra"""

        self.cassandra_data_list = data = self.common_utils.get_users_freeAPI(url)
        self.cassandra_adapter.insert_json_data(data=data, primary_key=primary_key, flatten=flatten)

    def migrate_to_mongo(self, database_name=None, collection_name=None, client_url=None, cred_file=None):
        """Migrate data from Cassandra to MongoDB"""
        
        mongo_adapter = MongoDB_CRUD(database_name=database_name, collection_name=collection_name, client_url=client_url, cred_file=cred_file)
        mongo_adapter.create_many(data_list=self.cassandra_data_list)
    
    def migrate_to_firebase_realtime(self, refrence_url, root_node, group_by=None, token=None):
        """Migrate data from Cassandra to Firebase realtime"""
        try:
            firebase_realtime_adapter = FirebaseRealtimeDatabaseCRUD(refrence_url=refrence_url, root_node=root_node, token=token)
            firebase_realtime_adapter.insert_json_data(data=self.cassandra_data_list, group_by_field=group_by)
        except Exception as e:
            print(e)
        except:
            print("Execution stopped")

    def migrate_to_firestore(self):
        pass