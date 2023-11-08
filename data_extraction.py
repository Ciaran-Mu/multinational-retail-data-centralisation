import pandas as pd
import tabula
import requests
from database_utils import DatabaseConnector
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import io
from tqdm import tqdm
# import grequests

class DataExtractor:
    
    def read_rds_table(self, db_connector: DatabaseConnector, table_name: str):
        engine = db_connector.init_db_engine()
        with engine.connect() as conn:
            return pd.read_sql_table(table_name, engine)
        # TODO: add error handling for ValueError Table ____ not found

    def retrieve_pdf_data(self, link: str):
        df_list = tabula.read_pdf(link, pages='all')
        data_frame = pd.concat(df_list, ignore_index=True)
        return data_frame
        # TODO: add error handling on link string input

    def list_number_of_stores(self, url: str, header: dict):
        endpoint = url + '/number_stores'
        response = requests.get(endpoint, headers=header)
        data = response.json()
        return data['number_stores']
        # TODO: add response code error handling

    def retrieve_stores_data(self, url: str, header: dict):
        number_stores = self.list_number_of_stores(url, header)

        data = []
        for store in tqdm(range(number_stores), desc='Retrieving stores data', unit='request'):
            endpoint = url + f'/store_details/{store}'
            response = requests.get(endpoint, headers=header)
            data.append(response.json())

        store_details = pd.DataFrame.from_records(data)
        return store_details
        # TODO: add response code error handling

    # def retrieve_stores_data_multi(self, url: str, header: dict):
    #     # Alternate function that performs requests in parallel with grequests
    #     # Requires import requests to be removed to avoid recursion error
    #     # Reduced time from 173 seconds to 112 seconds
    #     # Returned some 429 errors (though all 451 stores still came through)

    #     # number_stores = self.list_number_of_stores(url, header)
    #     data = []
    #     start_time = time.time()

    #     endpoints = [url + f'/store_details/{store}' for store in range(451)]
        
    #     multi_requests = (grequests.get(endpoint, headers=header) for endpoint in endpoints)
    #     responses = grequests.map(multi_requests)

    #     for response in responses:
    #         print(response.status_code)
    #         data.append(response.json())

    #     end_time = time.time()
    #     print(f'Requests took {end_time-start_time} seconds')
    #     store_details = pd.DataFrame.from_records(data)
    #     return store_details

    def extract_from_s3(self, s3_uri: str):
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

        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)

        return data_df