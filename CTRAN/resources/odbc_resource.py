import pyodbc
from dagster import ConfigurableResource
from typing import Optional

class ODBCResource(ConfigurableResource):
    username: str
    password: str
    server: str
    driver: str
    database: str
    port: Optional[int]

    def connect(self):
        if self.port:
            connectionString=f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};PORT={self.port};'
        else:
            connectionString=f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};TrustServerCertificate=yes;'
        return pyodbc.connect(connectionString)
