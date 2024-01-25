from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from tabulate import tabulate
import pandas as pd
import glob
from datamigrato.utils.common_utils import Common_utils


class CassandraCRUD:
    def __init__(self, keyspace_name, table_name, secure_bundle=None, token=None):
        self.common_utils = Common_utils()
        self.keyspace_name = keyspace_name
        self.table_name = table_name

        # Automatically detect secure_bundle and token if not provided
        try:
            if secure_bundle is None:
                secure_bundle_files = glob.glob('secure-connect-cassandra-*.zip')
                if len(secure_bundle_files) != 1:
                    raise FileNotFoundError("Unable to automatically determine secure bundle file.")
                secure_bundle = secure_bundle_files[0]

        except FileNotFoundError as e:
            print(e) 


        try:            
            if token is None:
                credentials_files = glob.glob('*-token.json')
                if len(credentials_files) != 1:
                    raise FileNotFoundError("Unable to automatically determine credentials file.")
                token = credentials_files[0]
        except FileNotFoundError as e:
            print(e)
        try:
            
            # Load cloud configuration and credentials
            cloud_config = {'secure_connect_bundle': secure_bundle}
            with open(token) as f:
                secrets = json.load(f)

            # Setup authentication
            auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect()

            # Connect to the specified keyspace
            self.session.set_keyspace(keyspace_name)
            print("Connected to Cassandra")
        except TypeError as e:
            print(f"Check your creds file: \n{e}")
        except Exception as e:
            print(f"Failed to connect to Cassandra: {e}")

    def create_table(self):
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id int PRIMARY KEY,
                name text
            );
            """
            self.session.execute(create_table_query)
            print("Table created successfully")
        except Exception as e:
            print(f"An error occurred during table creation: {e}")

    def list_tables(self):
        try:
            result = self.session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace_name}';")
            for row in result:
                print(row.table_name)
        except Exception as e:
            print(f"An error occurred: {e}")



    def create(self, query, parameters=None):
        try:
            self.session.execute(query, parameters)
            print("Data inserted successfully")
        except Exception as e:
            print(f"An error occurred during insertion: {e}")

    def read(self, query, parameters=None):
        try:
            return self.session.execute(query, parameters).one()
        except Exception as e:
            print(f"An error occurred during reading: {e}")

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

    def update(self, query, parameters=None):
        try:
            self.session.execute(query, parameters)
            print("Data updated successfully")
        except Exception as e:
            print(f"An error occurred during updating: {e}")

    def delete(self, query, parameters=None):
        try:
            self.session.execute(query, parameters)
            print("Data deleted successfully")
        except Exception as e:
            print(f"An error occurred during deletion: {e}")


    def show(self):
        query = f"SELECT * FROM {self.keyspace_name}.{self.table_name};"
        df = pd.DataFrame(list(self.session.execute(query)))
        print(tabulate(df, headers='keys', tablefmt='fancy_grid'))

    def delete_all_data(self):
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
            print(f"Table {self.table_name} created successfully with dynamic schema.")
        except Exception as e:
            print(f"An error occurred during dynamic table creation: {e}")

    def insert_json_data(self,data, primary_key = 'id', flatten=False):
        table_created = False
        try:
            for data_instance in data:
                # Convert ObjectId to string (or handle it as per your requirement)
                if primary_key in data_instance:
                    data_instance[primary_key] = str(data_instance[primary_key])

                # Flatten the data if required
                if flatten:
                    data_instance = self.common_utils.flatten_data(data_instance)

                if primary_key not in data_instance:
                    raise ValueError("Primary key not found in the provided data")

                # Create dynamic table if not already created
                if not table_created:
                    self.create_dynamic_table(data_instance.keys(), primary_key)
                    table_created = True

                # Prepare data for insertion
                cassandra_data = {k: str(v) for k, v in data_instance.items()}

                # Construct the query
                column_names = ', '.join([f'"{k}"' for k in cassandra_data.keys()])
                placeholders = ', '.join(['%s' for _ in cassandra_data])
                insert_query = f"INSERT INTO {self.keyspace_name}.{self.table_name} ({column_names}) VALUES ({placeholders})"
                self.create(insert_query, tuple(cassandra_data.values()))
        except TypeError as e:
            print(f"Error on reading tables {e}")
        except AttributeError as e:
            print(f"list object not created")