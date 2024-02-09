import glob
import firebase_admin
from firebase_admin import credentials, db, _apps

class FirebaseRealtimeDatabaseCRUD:
    def __init__(self, refrence_url, root_node, token=None):
        self.root_node = root_node
        
        token = token or self._detect_token()
        self.cred = credentials.Certificate(token)
        self.app = None

        if not _apps:
            self.app = firebase_admin.initialize_app(self.cred, {'databaseURL': refrence_url})
        self.root_node_ref = db.reference(self.root_node)

    def _detect_token(self):
        """Detects and returns the path to the token file."""
        credentials_files = glob.glob('*-firebase-adminsdk-*.json')
        if len(credentials_files) != 1:
            raise FileNotFoundError("Unable to automatically determine credentials file.")
        return credentials_files[0]
    
    def create(self, data):
        self.root_node_ref.set(data)
        print("Data created at root node")

    def read_all(self):
        return self.root_node_ref.get()

    def delete_records(self):
        self.root_node_ref.set({})
        print("All records deleted from root node")

    def delete_node(self):
        self.root_node_ref.delete()
        print("Node deleted from root node")

    def delete_database(self):
        db.reference('/').delete()
        print("Database deleted")

    def insert_json_data(self, data, group_by_field=None):
        """
        Inserts multiple JSON records into the database, optionally grouped by a specified field.

        Args:
            data (list): A list of dictionaries where each dictionary represents a record.
            group_by_field (str, optional): The field name to group the data by. If not provided, data is not grouped.
        """
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            print("Data should be a list of dictionaries.")
            return
        
        if group_by_field and not all(group_by_field in item for item in data):
            print(f"Some records are missing the '{group_by_field}' field.")
            return
        
        if group_by_field:
            formatted_data = self._group_data_by_field(data, group_by_field)
        else:
            formatted_data = {str(index): item for index, item in enumerate(data)}

        self.root_node_ref.update(formatted_data)
        print(f"Inserted {len(formatted_data)} records successfully.")

    def _group_data_by_field(self, data, group_by_field):
        """
        Groups data by the specified field.

        Args:
            data (list): A list of dictionaries where each dictionary represents a record.
            group_by_field (str): The field name to group the data by.

        Returns:
            dict: Grouped data.
        """
        grouped_data = {}
        for item in data:
            group_key = str(item.pop(group_by_field, None))  # Extract and remove the group_by_field
            if not group_key:
                print(f"Missing or invalid '{group_by_field}' in record: {item}")
                continue

            record_key = str(len(grouped_data.setdefault(group_key, {})))
            grouped_data[group_key][record_key] = item

        return grouped_data
