import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text

class DatabaseConnector:
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = db_creds['RDS_HOST']
        USER = db_creds['RDS_USER']
        PASSWORD = db_creds['RDS_PASSWORD']
        DATABASE = db_creds['RDS_DATABASE']
        PORT = db_creds['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        # TODO: change this to sqlalchemy inspect notation
        with engine.connect() as conn:
            tables = pd.read_sql_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", conn)
        print(tables)

    def upload_to_db(self, data_frame: pd.DataFrame, table_name: str):
        # This engine creation is bad repition, should be incorporated into init_db_engine
        # TODO: incorporate local engine into init_db_engine
        db_creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = db_creds['LOCAL_HOST']
        USER = db_creds['LOCAL_USER']
        PASSWORD = db_creds['LOCAL_PASSWORD']
        DATABASE = db_creds['LOCAL_DATABASE']
        PORT = db_creds['LOCAL_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        data_frame.to_sql(table_name, engine, if_exists='replace')