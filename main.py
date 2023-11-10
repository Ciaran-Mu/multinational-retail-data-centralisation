import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Initiate connection to legacy database
my_db = DatabaseConnector()
# List available tables in legacy database
print('Available tables in legacy database:')
my_db.list_db_tables()
# Class instance for data extraction
data_extractor = DataExtractor()

# Extract and clean the legacy users table then upload to the sales_data as dim_users
legacy_users_df = data_extractor.read_rds_table(my_db, 'legacy_users').set_index('index')
legacy_users_df = DataCleaning.clean_user_data(legacy_users_df)
# print(legacy_users_df.info())
my_db.upload_to_db(legacy_users_df, 'dim_users')
print('Extracting, cleaning and uploading legacy users table: Done')

# Extract card details from S3
card_details_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
card_details_df = DataCleaning.clean_card_data(card_details_df)
# print(card_details_df.info())
my_db.upload_to_db(card_details_df, 'dim_card_details')
print('Extracting, cleaning and uploading card details table: Done')

# # API connection
# header = {
#     'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
# }
# endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod'
# number_of_stores = data_extractor.list_number_of_stores(endpoint, header)
# print(f'Numer of stores in database: {number_of_stores}')

# store_details_df = data_extractor.retrieve_stores_data(endpoint, header)
# store_details_df = DataCleaning.clean_stores_data(store_details_df)
# # print(store_details_df.info())
# my_db.upload_to_db(store_details_df, 'dim_store_details')
# print('Extracting, cleaning and uploading store details table: Done')

# Product details from S3 using boto3
product_details_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
product_details_df = DataCleaning.clean_products_data(product_details_df)
# print(product_details_df.info())
my_db.upload_to_db(product_details_df, 'dim_products')
print('Extracting, cleaning and uploading product details table: Done')

# Extract and clean the orders table then upload to the sales_data as dim_users
orders_df = data_extractor.read_rds_table(my_db, 'orders_table').set_index('index')
orders_df = DataCleaning.clean_orders_data(orders_df)
# print(orders_df.info())
my_db.upload_to_db(orders_df, 'orders_table')
print('Extracting, cleaning and uploading orders table: Done')

# Date details from S3 using boto3
dates_df = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json')
dates_df = DataCleaning.clean_dates_data(dates_df)
# print(dates_df.info())
my_db.upload_to_db(dates_df, 'dim_date_times')
print('Extracting, cleaning and uploading date details table: Done')