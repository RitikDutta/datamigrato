from src.datamigrato.migration import mongodb_migrator


url = 'https://congenial-space-halibut-p7vr5gpwxqgh7qjx-8080.app.github.dev/api/v1/healthcheck'
database_name = 'database_test'
collection_name = 'collection_test'
client_url = "mongodb+srv://nisamfaras2:9JKFV21I5PvAZtgi@cluster0.sm6y67x.mongodb.net/?retryWrites=true&w=majority"

migrator = mongodb_migrator.Mongo_migrator(database_name, collection_name, client_url=client_url)
