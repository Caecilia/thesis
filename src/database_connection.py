import psycopg2

class DatabaseConnection:
	def __init__(self, user, password, host, database):
		self.user = user
		self.password = password
		self.host = host
		self.database = database

		try:
			connection_temp = psycopg2.connect(user = self.user,
								  password = self.password,
								  host = self.host,
								  database = self.database)
			self.connection = connection_temp

		except(Exception, psycopg2.Error) as error :
			print ("Error while connecting to PostgreSQL.", error)

	def get_connection(self):
		return self.connection

