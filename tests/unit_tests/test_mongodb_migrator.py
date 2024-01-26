from datamigrato.handler.datamigrato_handler import Datamigrato

class Test:
	"""docstring for test_mongo_migrator"""
	def __init__(self, bundle=None, token=None):
		self.client_url='mongodb+srv://nisamfaras2:9JKFV21I5PvAZtgi@cluster0.sm6y67x.mongodb.net/?retryWrites=true&w=majority'
		self.database_name='db_pytest'
		self.collection_name='col_pytest'
		self.handler = Datamigrato()
		self.populate_mongo() 
		self.mongo_to_cassandra(bundle=bundle, token=token)

	def getcreds(self):
		creds = 'creds'


	def populate_mongo(self):
		url = 'http://localhost:8080/api/v1/public/randomusers?page=1&limit=10'
		# url = 'https://literate-guide-5v6q74x5vw5c4q44-8080.app.github.dev/api/v1/public/randomusers?page=1&limit=10'
		self.handler.populate_mongo(url=url, client_url=self.client_url, database_name=self.database_name, collection_name=self.collection_name)
		assert True
	
	def mongo_to_cassandra(self, bundle=None, token=None):
		self.handler.mongo_to_cassandra(primary_key='id', client_url=self.client_url, database_name=self.database_name, collection_name=self.collection_name, keyspace_name=None, table_name=None, bundle=bundle, token=token, flatten=True)
		assert True