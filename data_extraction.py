from database_utils import DatabaseConnector
from botocore.exceptions import NoCredentialsError, ClientError
from tabula.io import read_pdf
from tqdm import tqdm

import pandas as pd
import requests
import boto3
import io


class DataExtractor:
    '''
    This class is used to extract data from multiple sources.
    '''
    
    def read_rds_table(self, db_connector: DatabaseConnector, table_name: str):
        '''
        This method reads a table of a given name from the connected database.

        Args:
            db_connector (DatabaseConnector): instance of connector class to either local or remote database
            table_name (str): name of table to read in database

        Returns:
            (DataFrame): pandas dataframe containing data from requested table
        '''
        engine = db_connector.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_table(table_name, conn)

    def retrieve_pdf_data(self, link: str):
        '''
        This method retrieves data from a PDF located at a given URL and returns the data in a pandas dataframe.
        Parsing of tabulated data in PDF is done using tabula-py module.

        Args:
            link (str): link to PDF hosted on the internet

        Returns:
            (DataFrame): pandas dataframe containing data retrieved from PDF
        '''
        df_list = read_pdf(link, pages='all')
        data_frame = pd.concat(df_list, ignore_index=True)
        return data_frame

    def list_number_of_stores(self, url: str, header: dict):
        '''
        This method requests the number of stores via an API hosted at a given URL.

        Args:
            url (str): link to the API
            header (dict): dictionary containing API key

        Returns:
            (int): number of stores
        '''
        endpoint = url + '/number_stores'
        response = requests.get(endpoint, headers=header)
        data = response.json()
        return data['number_stores']

    def retrieve_stores_data(self, url: str, header: dict):
        '''
        This method requests the details of each store via an API hosted at a given URL.

        Args:
            url (str): link to the API
            header (dict): dictionary containing API key

        Returns:
            store_details (DataFrame): details of all stores in a pandas dataframe
        '''
        # A note on the performance of this function:
        # Requesting all the info from each store sequentially takes a considerable amuont of time, this can be improved by using the grequests package to perform the requests in parallel
        # In testing this function written with the grequests package total time reduced from 173 to 112 seconds
        # However this resulted in many HTTP Error 429 (too many requests), so this parallel modification was deemed not worth it in this context and was removed

        number_stores = self.list_number_of_stores(url, header)

        response_data = []
        for store in tqdm(range(number_stores), desc='Retrieving stores data', unit='request'):
            endpoint = url + f'/store_details/{store}'
            response = requests.get(endpoint, headers=header)
            response_data.append(response.json())

        store_details = pd.DataFrame.from_records(response_data)
        return store_details

    def extract_from_s3(self, s3_uri: str):
        '''
        This method extracts data from AWS S3 objects in either CSV or JSON formats.

        Args:
            s3_uri (str): link to the object on AWS S3

        Returns:
            data_df (DataFrame): object data converted into a pandas dataframe
        '''
        try:
            # Boto3 code that may raise exceptions
            s3 = boto3.client('s3')
            
            #  Parse the S3 URI into bucket and object key
            s3_uri = s3_uri.replace('s3://', '')
            s3_bucket, s3_object = s3_uri.split('/', 1)

            # Process the response or perform other operations
            data = s3.get_object(Bucket=s3_bucket, Key=s3_object)
            if data['ContentType'] == 'text/csv':
                data_df = pd.read_csv(io.BytesIO(data['Body'].read()))
            elif data['ContentType'] == 'application/json':
                data_df = pd.read_json(io.BytesIO(data['Body'].read()))

            return data_df

        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)