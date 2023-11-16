import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
# from sqlalchemy import text

class DatabaseConnector:
    '''
    This class is used to create an object for data cleaning purposes: taking in data in the form of pandas dataframes and returning the same dataframes after cleaning

    Attributes:
        local (bool): set whether the instance of DatabaseConnector class will read the credentials for local or remote ('RDS')
    '''

    def __init__(self, local: bool = True):
        # Instance of this class is either a local ('LOCAL') or remote ('RDS') database connection
        if local == True:
            self.db = 'LOCAL'
        else:
            self.db = 'RDS'
    
    def __read_db_creds(self):
        '''
        This private method reads the database access credentials from a file "db_creds.yaml" saved in the working directory.

        Returns:
            db_creds (dict): credentials for both local and remote databases stored in a dictionary .
        '''
        # Read from credentials file using context manager and PyYaml module
        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self):
        '''
        This method connects to database via a SQLalchemy engine and returns that engine created.

        Returns:
            engine (engine): a SQLalchemy engine connected to a database.
        '''
        # Acquire database credentials
        db_creds = self.__read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = db_creds[f'{self.db}_HOST']
        USER = db_creds[f'{self.db}_USER']
        PASSWORD = db_creds[f'{self.db}_PASSWORD']
        DATABASE = db_creds[f'{self.db}_DATABASE']
        PORT = db_creds[f'{self.db}_PORT']
        # Initialise SQLalchemy engine
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        # engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def list_db_tables(self):
        '''
        This method lists the tables available in the connected database.
        Prints to terminal and has no returned variables.
        '''
        engine = self.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            inspector = inspect(conn)
            print(inspector.get_table_names())

    def upload_to_db(self, data_frame: pd.DataFrame, table_name: str):
        '''
        This method uploads a given pandas dataframe as a table to the connected database.
        If the table already exists in the database it will be replaced.

        Args:
            data_frame (DataFrame): data to be uploaded to table
            table_name (str): name for the uploaded table
        '''
        engine = self.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            data_frame.to_sql(table_name, conn, if_exists='replace', index=False)