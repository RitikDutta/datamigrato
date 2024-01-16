# src/handler/datamigrato_handler.py
# from datamigrato.migration.cassandra_migrator import CassandraCRUD
from datamigrato.migration.mongodb_migrator import Mongo_migrator

class Datamigrato:
    def __init__(self):
        print("Datamigrato class initialized")

    def mongo_to_cassandra(self, primary_key='_id', flatten=False):
        to_cassandra = Mongo_migrator()
        to_cassandra.migrate_to_cassandra(primary_key)
        print("done")