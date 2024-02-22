from datamigrato.handler.datamigrato_handler import Datamigrato

class Test:
	"""docstring for test_mongo_migrator"""
	def __init__(self, url, database_name, collection_name, keyspace_name, table_name, primary_key, flatten=False, client_url=None, cred_file=None, secure_bundle=None, cassandra_token=None):
		
		#cassandra params
		self.keyspace_name = keyspace_name
		self.table_name = table_name
		self.primary_key = primary_key
		self.secure_bundle = secure_bundle
		self.cassandra_token = cassandra_token

		#mongo params
		self.url = url
		self.database_name = database_name
		self.collection_name = collection_name
		self.client_url=client_url
		self.cred_file = cred_file

		self.handler = Datamigrato()
		self.populate_mongo(url, database_name, collection_name, client_url, cred_file)
		self.mongo_to_cassandra(database_name=self.database_name, collection_name=self.collection_name, client_url=self.client_url, cred_file=self.cred_file, keyspace_name=self.keyspace_name, table_name=self.table_name, primary_key=self.primary_key, secure_bundle=self.secure_bundle, cassandra_token=self.token, flatten=flatten)

	def getcreds(self):
		creds = 'creds'


	def populate_mongo(self, url, database_name, collection_name, client_url, cred_file):
		self.handler.populate_mongo(url=url, client_url=client_url, database_name=database_name, collection_name=collection_name, cred_file=cred_file)
		assert True
	
	def mongo_to_cassandra(self, database_name, collection_name, keyspace_name, table_name, primary_key='id', flatten=False, client_url=None, cred_file=None, secure_bundle=None, cassandra_token=None):
		self.handler.mongo_to_cassandra(primary_key=primary_key, client_url=self.client_url, cred_file=cred_file, database_name=self.database_name, collection_name=self.collection_name, keyspace_name=keyspace_name, table_name=table_name, secure_bundle=secure_bundle, token=cassandra_token, flatten=flatten)
		assert True