from typing import List
from sqlalchemy import create_engine, inspect

import credentials

class Database_Functions():
    def __init__(self) -> None:
        self.db_creds = credentials.get_pg_creds()
        self.host = self.db_creds['HOST']
        self.database = self.db_creds['DATABASE']
        self.port = self.db_creds['PORT']
        self.username = self.db_creds['USERNAME']
        self.pwd = self.db_creds['PWD']
        pass

    def get_engine(self):
        
        url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
            user=self.username, passwd=self.pwd, host=self.host, port=self.port, db=self.database)
        
        engine = create_engine(url, pool_size = 20, connect_args={"options": "-c timezone=America/Sao_Paulo"})
        return engine

    def list_tables(self, engine, schema: str = 'base_test') -> List:
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema=schema)
        return tables



database = Database_Functions()
engine_ref = database.get_engine()
tables = database.list_tables(engine=engine_ref)
print(tables)

't'