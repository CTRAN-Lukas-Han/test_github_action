import oracledb
from dagster import ConfigurableResource, get_dagster_logger
#from contextlib import contextmanager
#from pydantic import PrivateAttr

class INITDB:
    def __init__(self,username, password, connection_string):
        self.username=username
        self.password=password
        self.connection_string=connection_string

    def connect(self):
        self.connection = oracledb.connect(user=self.username,password=self.password,dsn=self.connection_string)

    def query(self,query):
        self.cursor = self.connection.cursor()
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.cursor.close()
        return rows

    def disconnect(self):
        self.connection.close()

class INITResource(ConfigurableResource):
    username: str
    password: str
    connection_string: str

    def query(self, body: str):
        logger = get_dagster_logger()
        logger.info("querying INIT: " + body)

        conn = INITDB(self.username,self.password,self.connection_string)
        conn.connect()
        rows = conn.query(body)
        conn.disconnect()
        return rows


    