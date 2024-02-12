from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from tabulate import tabulate
import pandas as pd
import glob
from datamigrato.utils.common_utils import Common_utils


class CassandraCRUD:
    def __init__(self, keyspace_name, table_name, secure_bundle=None, token=None):
        """
        Initializes the connection to a Cassandra database.
        """
        self.common_utils = Common_utils()
        self.keyspace_name = keyspace_name
        self.table_name = table_name

        # Detect secure_bundle and token if not provided
        secure_bundle = secure_bundle or self._detect_secure_bundle()
        token = token or self._detect_token()

        # Load cloud configuration and credentials
        try:
            cloud_config = {'secure_connect_bundle': secure_bundle}
            with open(token) as f:
                secrets = json.load(f)

            # Setup authentication and connect to Cassandra
            auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect(self.keyspace_name)
            print("Connected to Cassandra")
        except Exception as e:
            print(f"Failed to connect to Cassandra: {e}")

    def _detect_secure_bundle(self):
        """Detects and returns the path to the secure bundle."""
        secure_bundle_files = glob.glob('secure-connect-cassandra-*.zip')
        if len(secure_bundle_files) != 1:
            raise FileNotFoundError("Unable to automatically determine secure bundle file.")
        return secure_bundle_files[0]

    def _detect_token(self):
        """Detects and returns the path to the token file."""
        credentials_files = glob.glob('*-token.json')
        if len(credentials_files) != 1:
            raise FileNotFoundError("Unable to automatically determine credentials file.")
        return credentials_files[0]


    def create(self, query, parameters=None):
        try:
            self.session.execute(query, parameters)
        except Exception as e:
            print(f"An error occurred during insertion: {e}")

    def read_all(self):
        try:
            query = f"SELECT * FROM {self.keyspace_name}.{self.table_name};"
            result_set = self.session.execute(query)
            records = []

            # Convert each row in the result set to a dictionary
            for row in result_set:
                record = {column: getattr(row, column) for column in row._fields}
                records.append(record)

            return records
        except Exception as e:
            print(f"An error occurred during reading all data: {e}")
            return []

    def show(self):
        query = f"SELECT * FROM {self.keyspace_name}.{self.table_name};"
        df = pd.DataFrame(list(self.session.execute(query)))
        print(tabulate(df, headers='keys', tablefmt='fancy_grid'))

    def delete_records(self):
        try:
            delete_query = f"TRUNCATE {self.keyspace_name}.{self.table_name};"
            self.session.execute(delete_query)
            print(f"All data from table '{self.table_name}' in keyspace '{self.keyspace_name}' has been deleted.")
        except Exception as e:
            print(f"An error occurred during deleting all data: {e}")
    def delete_table(self):
        try:
            drop_table_query = f"DROP TABLE IF EXISTS {self.keyspace_name}.{self.table_name};"
            self.session.execute(drop_table_query)
            print(f"Table {self.table_name} in keyspace {self.keyspace_name} has been deleted.")
        except Exception as e:
            print(f"An error occurred during table deletion: {e}")


    def create_dynamic_table(self, fields, primary_key):
        # Ensure there are fields other than the primary key
        if not fields or len(fields) <= 1:
            raise ValueError("Insufficient fields to create a table")

        # Construct columns, excluding the primary key
        columns = [f'"{field}" text' for field in fields if field != primary_key]
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace_name}.{self.table_name} (
                "{primary_key}" text PRIMARY KEY,
                {', '.join(columns)}
            );
        """
        try:
            self.session.execute(create_table_query)
        except Exception as e:
            print(f"An error occurred during dynamic table creation: {e}")

    def insert_json_data(self, data, primary_key='id', flatten=False):
        """Inserts JSON data into the Cassandra table, creating the table dynamically if needed."""
        inserted_count = 0
        try:
            table_created = self._create_table_if_needed(data, primary_key)
            for data_instance in data:
                self._process_and_insert_data_instance(data_instance, primary_key, flatten, table_created)
                inserted_count += 1
            print(f"Inserted {inserted_count} records successfully.")
        except TypeError as e:
            print(f"Error processing data: {e}")
        except ValueError as e:
            print(e)

    def _create_table_if_needed(self, data, primary_key):
        """Creates a dynamic table based on the data's keys if it has not been created yet."""
        if data and isinstance(data, list):
            first_data_instance = data[0]
            self.create_dynamic_table(first_data_instance.keys(), primary_key)
            return True
        return False

    def _process_and_insert_data_instance(self, data_instance, primary_key, flatten, table_created):
        """Processes a single data instance and inserts it into the Cassandra table."""
        if primary_key not in data_instance:
            raise ValueError("Primary key not found in the provided data")

        if flatten:
            data_instance = self.common_utils.flatten_data(data_instance)

        cassandra_data = {k: str(v) for k, v in data_instance.items()}
        self._insert_data_into_cassandra(cassandra_data)

    def _insert_data_into_cassandra(self, cassandra_data):
        """Constructs and executes an INSERT query to insert data into the Cassandra table."""
        column_names = ', '.join([f'"{k}"' for k in cassandra_data.keys()])
        placeholders = ', '.join(['%s' for _ in cassandra_data])
        insert_query = f"INSERT INTO {self.keyspace_name}.{self.table_name} ({column_names}) VALUES ({placeholders})"
        self.create(insert_query, tuple(cassandra_data.values()))
