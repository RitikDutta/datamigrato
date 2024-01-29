from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError

class MongoDB_CRUD:
    """Handles CRUD operations for a MongoDB Database."""

    def __init__(self, client_url, database_name, collection_name):
        """Initializes MongoDB client and connects to the specified collection."""
        try:
            self.client = MongoClient(client_url)
            self.client.server_info()  # Validates the connection
            self.db = self.client[database_name]
            self.collection = self.db[collection_name]
            print("Connected to MongoDB")
            self.is_connected = True
        except ConnectionFailure:
            print("Failed to connect to MongoDB. Check IP and credentials.")
            self.is_connected = False
        except PyMongoError as e:
            print(f"MongoDB connection error: {e}")
            self.is_connected = False

    def create_many(self, data_list):
        """Inserts multiple documents into the collection."""
        try:
            if not isinstance(data_list, list) or not all(isinstance(item, dict) for item in data_list):
                raise ValueError("Input should be a list of dictionaries")
            result = self.collection.insert_many(data_list)
            print(f"Inserted {len(result.inserted_ids)} documents successfully")
        except PyMongoError as e:
            print(f"Insertion error: {e}")
        except TypeError as e:
            print(f"Typer Error: {e}")

    def read_all(self):
        """Reads and returns all documents from the collection."""
        try:
            return list(self.collection.find({}))
        except PyMongoError as e:
            print(f"Read error: {e}")
            return None

    def delete_records(self):
        """Deletes all records from the collection."""
        try:
            self.collection.delete_many({})
            print("All data deleted successfully")
        except PyMongoError as e:
            print(f"Deletion error: {e}")

    def delete_collection(self):
        """Drops the entire collection."""
        try:
            self.db.drop_collection(self.collection.name)
            print(f"Collection '{self.collection.name}' deleted successfully")
        except PyMongoError as e:
            print(f"Error deleting collection: {e}")

    def delete_database(self):
        """Drops the entire database."""
        try:
            self.client.drop_database(self.db.name)
            print(f"Database '{self.db.name}' deleted successfully")
        except PyMongoError as e:
            print(f"Error deleting database: {e}")
