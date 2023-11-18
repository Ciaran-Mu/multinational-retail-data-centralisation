import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text

class DatabaseConnector:
    '''
    This class is used to connect to a database, and perform some basic functionality such as listing tables and uploading tables.

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
            db_creds (dict): credentials for both local and remote databases stored in a dictionary.
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
            data_frame (DataFrame): data to be uploaded to table.
            table_name (str): name for the uploaded table.
        '''
        engine = self.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            data_frame.to_sql(table_name, conn, if_exists='replace', index=False)


class DatabaseModifier:
    '''
    This class is used to modify tables in an existing database.
    All queries written in PostgreSQL.

    Attributes:
        db_connector (DatabaseConnector): set whether the instance of DatabaseConnector class will read the credentials for local or remote ('RDS')
    '''

    def __init__(self,  db_connector: DatabaseConnector):
        # Instance of this class needs a database connector to either a local or remote database
        self.db_connector = db_connector

    def alter_column_type(self, table_name: str, column_name: str, column_type: str, varchar_length: int = 0):
        '''
        This method changes the data type of a column in the database using a SQL query.
        For the case of changing the type to VARCHAR either a length can be provided in arguements or the maximum length of records currently in that column will be used.

        Args:
            table_name (str): name of table in the database.
            column_name (str): name of column to change type.
            column_type (str): data type to set column to, must be one of ('UUID', 'FLOAT', 'VARCHAR', 'SMALLINT', 'BIGINT', 'DATE', 'TEXT').
            [optional] varchar_length (int): number of characters for setting VARCHAR type. Default is automatically set to longest string in records.
        '''
        # Catch errors in the column_type provided (note this is not extensive coverage of all SQL data types)
        if column_type not in ('UUID', 'FLOAT', 'VARCHAR', 'SMALLINT', 'BIGINT', 'DATE', 'TEXT'):
            raise ValueError('Incorrect value for column type')

        engine = self.db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            if column_type == 'UUID':
                conn.execute(text(f'''
                                    ALTER TABLE {table_name}
                                        ALTER COLUMN {column_name} TYPE UUID USING {column_name}::UUID;
                                '''))
            
            elif column_type == 'FLOAT':
                conn.execute(text(f'''
                                    ALTER TABLE {table_name}
                                        ALTER COLUMN {column_name} TYPE FLOAT(24);
                                '''))
                
            elif column_type == 'VARCHAR':
                if  varchar_length == 0:
                    max_length_query = conn.execute(text(f'''
                                                            SELECT MAX(LENGTH({column_name}::TEXT))
                                                            FROM {table_name};
                                                        '''))
                    varchar_length = max_length_query.all()[0][0]

                conn.execute(text(f'''
                                    ALTER TABLE {table_name}
                                        ALTER COLUMN {column_name} TYPE VARCHAR({varchar_length});
                                '''))

            else:
                conn.execute(text(f'''
                                    ALTER TABLE {table_name}
                                        ALTER COLUMN {column_name} TYPE {column_type};
                                '''))
                
    def alter_column_text_to_bool(self, table_name: str, column_name: str, true_string: str):
        '''
        This method is specifically for changing the data type of a column from text to bool in the database using a SQL query.
        The text records matching the 'true_string' arguement will be the true values in the resulting boolean column.

        Args:
            table_name (str): name of table in the database.
            column_name (str): name of column to change type.
            true_string (str): string to match as the true value records.
        '''
        engine = self.db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(text(f'''
                                DO $$
                                BEGIN
                                    IF (SELECT data_type
                                        FROM information_schema.columns
                                        WHERE table_name = '{table_name}' AND column_name = '{column_name}') = 'text'
                                    THEN
                                        UPDATE {table_name}
                                            SET {column_name} = ({column_name} = '{true_string}')::BOOL;
                                        ALTER TABLE {table_name}
                                            ALTER COLUMN {column_name} TYPE BOOL USING {column_name}::BOOL;
                                    END IF;
                                END $$;
                            '''))

    def alter_column_name(self, table_name: str, old_column_name: str, new_column_name: str):
        '''
        This method changes the name of a column in the database using a SQL query.

        Args:
            table_name (str): name of table in the database.
            old_column_name (str): name of column existing in the database to change.
            new_column_name (str): new name of the chosen column.
        '''
        engine = self.db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(text(f'''  
                                DO $$
                                BEGIN
                                    IF EXISTS(SELECT *
                                            FROM information_schema.columns
                                            WHERE table_name = '{table_name}' AND column_name = '{old_column_name}')
                                    THEN
                                        ALTER TABLE {table_name}
                                            RENAME COLUMN {old_column_name} TO {new_column_name};
                                    END IF;
                                END $$;
                            '''))
            
    def add_column(self, table_name: str, column_name: str, column_type: str):
        '''
        This method adds a new column to table in the database using a SQL query.

        Args:
            table_name (str): name of table in the database.
            old_column_name (str): name of column to add.
        '''
        engine = self.db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(text(f'''
                                ALTER TABLE {table_name}
                                    ADD COLUMN IF NOT EXISTS {column_name} {column_type};
                            '''))
            
    def update_weight_class(self, table_name: str):
        '''
        This is a specialised method populates a weight class based on a weight column table in the database using a SQL query.

        Args:
            table_name (str): name of table in the database.
        '''
        engine = self.db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(text(f'''
                                UPDATE {table_name}
                                    SET
                                        weight_class = 'Light'
                                    WHERE weight < 2;
                            '''))
            
            conn.execute(text(f'''
                                UPDATE {table_name}
                                    SET
                                        weight_class = 'Mid_Sized'
                                    WHERE weight >= 2 AND weight < 40;
                            '''))
            
            conn.execute(text(f'''
                                UPDATE {table_name}
                                    SET
                                        weight_class = 'Heavy'
                                    WHERE weight >= 40 AND weight < 140;
                            '''))
            
            conn.execute(text(f'''
                                UPDATE {table_name}
                                    SET
                                        weight_class = 'Truck_Required'
                                    WHERE weight >= 140;
                            '''))
    
    def set_primary_key(self, table_name: str, column_name: str):
        '''
        This method sets a column as the primary key for a table in the database using a SQL query.

        Args:
            table_name (str): name of table in the database.
            column_name (str): name of column to be the primary key.
        '''
        engine = self.db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(text(f'''
                                DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY')
                                    THEN
                                        ALTER TABLE {table_name}
                                            ADD PRIMARY KEY ({column_name});
                                    END IF;
                                END $$;
                            '''))
    
    def set_foreign_key(self, table_name: str, column_name: str, ref_table_name: str):
        '''
        This method sets a foreign key for a particular column in a table with reference to primary key in another using a SQL query.

        Args:
            table_name (str): name of table in the database.
            column_name (str): name of column to be the foreign key (same as column name of primary key in the reference table).
            table_name (str): name of reference table for the foreign key in the database.
        '''
        engine = self.db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(text(f'''
                                DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = '{table_name}' AND constraint_name = '{table_name}_{column_name}_fkey')
                                    THEN
                                        ALTER TABLE {table_name}
                                            ADD FOREIGN KEY ({column_name})
                                            REFERENCES {ref_table_name}({column_name});
                                    END IF;
                                END $$;
                            '''))