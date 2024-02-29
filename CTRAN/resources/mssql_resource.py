from dagster import ConfigurableResource
import pymssql

class MSSQLDB:
    def __init__(self,username, password, host,db):
        self.username=username
        self.password=password
        self.host=host
        self.database=db

    def connect(self):
        self.connection = pymssql.connect(host=self.host,user=self.username,password=self.password,database=self.database)

    def query(self,query):
        self.cursor = self.connection.cursor()
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.cursor.close()
        return rows

    def disconnect(self):
        self.connection.close()

class MSSQLResource(ConfigurableResource):
    username: str
    password: str
    host: str
    database: str

    def query(self, body: str):
        conn = MSSQLDB(host=self.host,username=self.username,password=self.password,db=self.database)
        conn.connect()
        rows = conn.query(body)
        conn.disconnect()
        return rows


    