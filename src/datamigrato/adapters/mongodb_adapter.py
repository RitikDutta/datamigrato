from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError


class MongoDB_CRUD:
    def __init__(self, client_url, database_name, collection_name):
        try:
            client_url = "mongodb+srv://nisamfaras2:9JKFV21I5PvAZtgi@cluster0.sm6y67x.mongodb.net/?retryWrites=true&w=majority"
            database_name = 'database_test'
            collection_name = 'collection_test'
            self.client = MongoClient(client_url)
            self.client.server_info()  # Trigger exception if cannot connect to database
            self.db = self.client[database_name]
            self.collection = self.db[collection_name]
            print("Connected to MongoDB")
        except ConnectionFailure:
            print("Failed to connect to MongoDB")
        except PyMongoError as e:
            print(f"An error occurred: {e}")

    def create(self, data):
        try:
            self.collection.insert_one(data)
            print("Data inserted successfully")
        except PyMongoError as e:
            print(f"An error occurred during insertion: {e}")

    def read(self, query):
        try:
            return self.collection.find_one(query)
        except PyMongoError as e:
            print(f"An error occurred during reading: {e}")
            return None

    def read_all(self):
        try:
            return list(self.collection.find({}))
        except PyMongoError as e:
            print(f"An error occurred during reading all documents: {e}")
            return None

    def update(self, query, new_values):
        try:
            self.collection.update_one(query, {"$set": new_values})
            print("Data updated successfully")
        except PyMongoError as e:
            print(f"An error occurred during updating: {e}")

    def delete(self, query):
        try:
            self.collection.delete_one(query)
            print("Data deleted successfully")
        except PyMongoError as e:
            print(f"An error occurred during deletion: {e}")

    def delete_all(self):
        try:
            self.collection.delete_many({})
            print("All data deleted successfully")
        except PyMongoError as e:
            print(f"An error occurred during deletion: {e}")

    def delete_collection(self):
        try:
            self.db.drop_collection(self.collection.name)
            print(f"Collection '{self.collection.name}' deleted successfully")
        except PyMongoError as e:
            print(f"An error occurred during deleting collection: {e}")

    def delete_database(self):
        try:
            self.client.drop_database(self.db.name)
            print(f"Database '{self.db.name}' deleted successfully")
        except PyMongoError as e:
            print(f"An error occurred during deleting database: {e}")
