import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from sqlalchemy import text
from tabulate import tabulate

# Initiate connection to legacy database
local_db = DatabaseConnector(local=True)
remote_db = DatabaseConnector(local=False)
# Class instance for data extraction
data_extractor = DataExtractor()

# Extracting and cleaning data
print('Section 1: Extracting and cleaning data')
print('--------------------------------------------------------------------')

# List available tables in legacy database
print('\nAvailable tables in legacy database:')
remote_db.list_db_tables()

# Extract and clean the legacy users table then upload to the sales_data as dim_users
legacy_users_df = data_extractor.read_rds_table(remote_db, 'legacy_users').set_index('index')
legacy_users_df = DataCleaning.clean_user_data(legacy_users_df)
# print(legacy_users_df.info())
local_db.upload_to_db(legacy_users_df, 'dim_users')
print('\nExtracting, cleaning and uploading legacy users table: Done')

# Extract card details from S3
card_details_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
card_details_df = DataCleaning.clean_card_data(card_details_df)
# print(card_details_df.info())
local_db.upload_to_db(card_details_df, 'dim_card_details')
print('\nExtracting, cleaning and uploading card details table: Done')

# API connection
header = {
    'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
}
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod'
number_of_stores = data_extractor.list_number_of_stores(endpoint, header)
print(f'\nNumber of stores in database: {number_of_stores}')

store_details_df = data_extractor.retrieve_stores_data(endpoint, header)
store_details_df = DataCleaning.clean_stores_data(store_details_df)
# print(store_details_df.info())
local_db.upload_to_db(store_details_df, 'dim_store_details')
print('Extracting, cleaning and uploading store details table: Done')

# Product details from S3 using boto3
product_details_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
product_details_df = DataCleaning.clean_products_data(product_details_df)
# print(product_details_df.info())
local_db.upload_to_db(product_details_df, 'dim_products')
print('\nExtracting, cleaning and uploading product details table: Done')

# Extract and clean the orders table then upload to the sales_data as dim_users
orders_df = data_extractor.read_rds_table(remote_db, 'orders_table').set_index('index')
orders_df = DataCleaning.clean_orders_data(orders_df)
# print(orders_df.info())
local_db.upload_to_db(orders_df, 'orders_table')
print('\nExtracting, cleaning and uploading orders table: Done')

# Date details from S3 using boto3
dates_df = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json')
dates_df = DataCleaning.clean_dates_data(dates_df)
# print(dates_df.info())
local_db.upload_to_db(dates_df, 'dim_date_times')
print('\nExtracting, cleaning and uploading date details table: Done\n')



# Developing the database schema using SQL
print('Section 2: Developing the database schema using SQL')
print('--------------------------------------------------------------------')

# TODO: refactor these commands into funciton to avoid repetition
engine = local_db.init_db_engine()

# Modify the orders table
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text('''ALTER TABLE orders_table
                            ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;'''))
    
    conn.execute(text('''ALTER TABLE orders_table
                            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;'''))
    
    conn.execute(text('''ALTER TABLE orders_table
                            ALTER COLUMN card_number TYPE TEXT;'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(card_number))
                                            FROM orders_table;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE orders_table
                            ALTER COLUMN card_number TYPE VARCHAR({varchar_length});'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(store_code))
                                            FROM orders_table;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE orders_table
                            ALTER COLUMN store_code TYPE VARCHAR({varchar_length});'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(product_code))
                                            FROM orders_table;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE orders_table
                            ALTER COLUMN product_code TYPE VARCHAR({varchar_length});'''))
    
    conn.execute(text('''ALTER TABLE orders_table
                            ALTER COLUMN product_quantity TYPE SMALLINT;'''))

print('\nModifying the orders table: Done')

# Modify the users table
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text(f'''ALTER TABLE dim_users
                            ALTER COLUMN first_name TYPE VARCHAR(255);'''))
    
    conn.execute(text(f'''ALTER TABLE dim_users
                            ALTER COLUMN last_name TYPE VARCHAR(255);'''))
    
    conn.execute(text(f'''ALTER TABLE dim_users
                            ALTER COLUMN date_of_birth TYPE DATE;'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(country_code))
                                            FROM dim_users;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_users
                            ALTER COLUMN country_code TYPE VARCHAR({varchar_length});'''))
    
    conn.execute(text('''ALTER TABLE dim_users
                            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;'''))
    
    conn.execute(text(f'''ALTER TABLE dim_users
                            ALTER COLUMN join_date TYPE DATE;'''))

print('\nModifying the users table: Done')
    
# Modifiy the stores table
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN longitude TYPE FLOAT(24);'''))
    
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN locality TYPE VARCHAR(255);'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(store_code))
                                            FROM dim_store_details;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN store_code TYPE VARCHAR({varchar_length});'''))
    
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN staff_numbers TYPE SMALLINT;'''))
    
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN opening_date TYPE DATE;'''))
    
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN store_type TYPE VARCHAR(255);'''))

    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN latitude TYPE FLOAT(24);'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(country_code))
                                            FROM dim_store_details;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN country_code TYPE VARCHAR({varchar_length});'''))
    
    conn.execute(text(f'''ALTER TABLE dim_store_details
                            ALTER COLUMN continent TYPE VARCHAR(255);'''))
    
    # All columns that are varchar/text are already N/A for web portal, lat and long are not compatible with 'N/A'

    # conn.execute(text(f'''UPDATE dim_store_details
    #                         SET country_code = NULL,
    #                       WHERE
    #                         store_type = 'Web Portal';'''))

print('\nModifying the stores table: Done')

# Modify the products table
# Price already has £ removed
# Add weight class
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text(f'''  ALTER TABLE dim_products
	                            ADD COLUMN IF NOT EXISTS weight_class VARCHAR(14);'''))
    
    conn.execute(text(f'''  UPDATE dim_products
                            SET
                                weight_class = 'Light'
                            WHERE weight < 2;'''))
    
    conn.execute(text(f'''  UPDATE dim_products
                            SET
                                weight_class = 'Mid_Sized'
                            WHERE weight >= 2 AND weight < 40;'''))
    
    conn.execute(text(f'''  UPDATE dim_products
                            SET
                                weight_class = 'Heavy'
                            WHERE weight >= 40 AND weight < 140;'''))
    
    conn.execute(text(f'''  UPDATE dim_products
                            SET
                                weight_class = 'Truck_Required'
                            WHERE weight >= 140;'''))
# Change column types
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text(f'''ALTER TABLE dim_products
                            ALTER COLUMN product_price TYPE FLOAT(24);'''))
    
    conn.execute(text(f'''ALTER TABLE dim_products
                            ALTER COLUMN weight TYPE FLOAT(24);'''))

    max_length_query = conn.execute(text('''SELECT MAX(LENGTH("EAN"::TEXT))
                                            FROM dim_products;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_products
                            ALTER COLUMN "EAN" TYPE VARCHAR({varchar_length});'''))

    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(product_code))
                                            FROM dim_products;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_products
                            ALTER COLUMN product_code TYPE VARCHAR({varchar_length});'''))
    
    conn.execute(text(f'''ALTER TABLE dim_products
                            ALTER COLUMN date_added TYPE DATE;'''))
    
    conn.execute(text(f'''ALTER TABLE dim_products
                            ALTER COLUMN uuid TYPE UUID USING uuid::UUID;'''))
    
    conn.execute(text(f'''  DO $$
                            BEGIN
                                IF EXISTS(SELECT *
                                        FROM information_schema.columns
                                        WHERE table_name = 'dim_products' AND column_name = 'removed')
                                THEN
                                    ALTER TABLE dim_products
                                        RENAME COLUMN removed TO still_available;
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                            BEGIN
                                IF (SELECT data_type
                                    FROM information_schema.columns
                                    WHERE table_name = 'dim_products' AND column_name = 'still_available') = 'text'
                                THEN
                                    UPDATE dim_products
                                        SET still_available = (still_available = 'Still_avaliable')::BOOL;
                                    ALTER TABLE dim_products
                                        ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL;
                                END IF;
                            END $$;'''))

print('\nModifying the products table: Done')
    
# Modify the dates table
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text(f'''ALTER TABLE dim_date_times
                            ALTER COLUMN month TYPE VARCHAR(2);'''))
    
    conn.execute(text(f'''ALTER TABLE dim_date_times
                            ALTER COLUMN year TYPE VARCHAR(4);'''))
    
    conn.execute(text(f'''ALTER TABLE dim_date_times
                            ALTER COLUMN day TYPE VARCHAR(2);'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(time_period))
                                            FROM dim_date_times;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_date_times
                            ALTER COLUMN time_period TYPE VARCHAR({varchar_length});'''))
    
    conn.execute(text(f'''ALTER TABLE dim_date_times
                            ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;'''))

print('\nModifying the dates table: Done')

# Modify the card details table
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(card_number::TEXT))
                                            FROM dim_card_details;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_card_details
                            ALTER COLUMN card_number TYPE VARCHAR({varchar_length});'''))
    
    max_length_query = conn.execute(text('''SELECT MAX(LENGTH(expiry_date::TEXT))
                                            FROM dim_card_details;'''))
    varchar_length = max_length_query.all()[0][0]
    conn.execute(text(f'''ALTER TABLE dim_card_details
                            ALTER COLUMN expiry_date TYPE VARCHAR({varchar_length});'''))
    
    conn.execute(text(f'''ALTER TABLE dim_card_details
                            ALTER COLUMN date_payment_confirmed TYPE DATE;'''))
    
print('\nModifying the card details table: Done')

input('''   \nThe next step is linking the tables together in the star-based schema by assigning the primary and foreign keys
            \nNote: after the keys have been assigned the previous code above cannot be run again without manually dropping all tables first
            \nDo you want to continue? Press Enter''')

# Assign the primary keys in all the dimensions (dim) tables
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'dim_date_times' AND constraint_type = 'PRIMARY KEY')
                                    THEN
                                        ALTER TABLE dim_date_times
                                            ADD PRIMARY KEY (date_uuid);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'dim_users' AND constraint_type = 'PRIMARY KEY')
                                    THEN
                                        ALTER TABLE dim_users
                                            ADD PRIMARY KEY (user_uuid);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'dim_card_details' AND constraint_type = 'PRIMARY KEY')
                                    THEN
                                        ALTER TABLE dim_card_details
                                            ADD PRIMARY KEY (card_number);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'dim_store_details' AND constraint_type = 'PRIMARY KEY')
                                    THEN
                                        ALTER TABLE dim_store_details
                                            ADD PRIMARY KEY (store_code);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'dim_products' AND constraint_type = 'PRIMARY KEY')
                                    THEN
                                        ALTER TABLE dim_products
                                            ADD PRIMARY KEY (product_code);
                                END IF;
                            END $$;'''))
    
# Assign the foreign keys in the orders table
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'orders_table' AND constraint_name = 'orders_table_date_uuid_fkey')
                                    THEN
                                        ALTER TABLE orders_table
                                            ADD FOREIGN KEY (date_uuid)
                                            REFERENCES dim_date_times(date_uuid);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'orders_table' AND constraint_name = 'orders_table_user_uuid_fkey')
                                    THEN
                                        ALTER TABLE orders_table
                                            ADD FOREIGN KEY (user_uuid)
                                            REFERENCES dim_users(user_uuid);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'orders_table' AND constraint_name = 'orders_table_card_number_fkey')
                                    THEN
                                        ALTER TABLE orders_table
                                            ADD FOREIGN KEY (card_number)
                                            REFERENCES dim_card_details(card_number);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'orders_table' AND constraint_name = 'orders_table_store_code_fkey')
                                    THEN
                                        ALTER TABLE orders_table
                                            ADD FOREIGN KEY (store_code)
                                            REFERENCES dim_store_details(store_code);
                                END IF;
                            END $$;'''))
    
    conn.execute(text(f'''  DO $$
                                BEGIN
                                    IF NOT EXISTS(	SELECT *
                                                    FROM information_schema.table_constraints
                                                    WHERE table_name = 'orders_table' AND constraint_name = 'orders_table_product_code_fkey')
                                    THEN
                                        ALTER TABLE orders_table
                                            ADD FOREIGN KEY (product_code)
                                            REFERENCES dim_products(product_code);
                                END IF;
                            END $$;'''))
    
print('\nCreating the star-based schema for tables: Done\n')

# Querying the database using SQL
print('Section 3: Querying the database')
print('--------------------------------------------------------------------')

engine = local_db.init_db_engine()

print('\nQuery 1: How many stores does the business have and in which countries?')
# Task 1
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT  country_code,
                            COUNT(store_code) AS total_no_stores
                    FROM dim_store_details
                    GROUP BY country_code
                    ORDER BY total_no_stores DESC;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 2: Which locations currently have the most stores?')
# Task 2
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT  locality,
                            COUNT(store_code) AS total_no_stores
                    FROM dim_store_details
                    GROUP BY locality
                    ORDER BY total_no_stores DESC
                    LIMIT 7;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 3: Which months produced the largest amount of sales?')
# Task 3
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT	SUM(product_quantity * product_price) AS total_sales,
                        month
                    FROM orders_table
                    JOIN
                        dim_products ON orders_table.product_code = dim_products.product_code
                    JOIN
                            dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
                    GROUP BY month
                    ORDER BY total_sales DESC
                    LIMIT 6;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 4: How many sales are coming from online?')
# Task 4
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT	COUNT(date_uuid) AS nnumber_of_sales,
                            SUM(product_quantity) AS product_quantity_count,
                            CASE
                                WHEN store_type = 'Web Portal' THEN 'Web'
                                ELSE 'Offline'
                            END AS location
                    FROM orders_table
                    JOIN
                        dim_store_details ON orders_table.store_code = dim_store_details.store_code
                    GROUP BY location
                    ORDER BY location DESC;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 5: What percentage of sales come through each type of store?')
# Task 5
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT	store_type,
                            SUM(product_quantity * product_price) AS total_sales,
                            SUM(product_quantity * product_price) / (
                                SELECT	SUM(product_quantity * product_price)
                                FROM orders_table
                                JOIN
                                    dim_products ON orders_table.product_code = dim_products.product_code
                            ) * 100 AS percentage_total
                    FROM orders_table
                    JOIN
                        dim_products ON orders_table.product_code = dim_products.product_code
                    JOIN
                        dim_store_details ON orders_table.store_code = dim_store_details.store_code
                    GROUP BY store_type
                    ORDER BY percentage_total DESC;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 6: Which month in each year produced the highest cost of sales?')
# Task 6
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT	SUM(product_quantity * product_price) AS total_sales,
                            year,
                            month
                    FROM orders_table
                    JOIN
                        dim_products ON orders_table.product_code = dim_products.product_code
                    JOIN
                        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
                    GROUP BY year, month
                    ORDER BY total_sales DESC
                    LIMIT 10;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

print('''\nVariation of query 6: Which month in each year produced the highest cost of sales?
            \nIncluding only the highest earning months for each year''')
# Task 6 variation - provide only the most performant month per each year
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    WITH sales_by_year AS (
                        SELECT	SUM(product_quantity * product_price) AS total_sales,
                                year,
                                month
                        FROM orders_table
                        JOIN
                            dim_products ON orders_table.product_code = dim_products.product_code
                        JOIN
                            dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
                        GROUP BY year, month
                    ) SELECT	total_sales,
                                year,
                                month
                    FROM sales_by_year
                    WHERE total_sales IN (
                            SELECT MAX(total_sales) OVER (PARTITION BY year) AS final_sales
                            FROM sales_by_year
                    )
                    ORDER BY total_sales DESC
                    LIMIT 10;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 7: What is our staff headcount?')
# Task 7
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT	SUM(staff_numbers) AS total_staff_numbers,
                            country_code
                    FROM dim_store_details
                    GROUP BY country_code
                    ORDER BY total_staff_numbers DESC;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 8: Which German store type is selling the most?')
# Task 8
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    SELECT	SUM(product_quantity * product_price) AS total_sales,
                            store_type,
                            country_code		
                    FROM orders_table
                    JOIN
                        dim_products ON orders_table.product_code = dim_products.product_code
                    JOIN
                        dim_store_details ON orders_table.store_code = dim_store_details.store_code
                    GROUP BY country_code, store_type
                    HAVING country_code = 'DE'
                    ORDER BY total_sales;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

input('\nContinue to next query? Press Enter')

print('\nQuery 9: How quickly is the company making sales?')
# Task 9
with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    result = pd.read_sql_query('''
                    WITH sales_intervals AS (
                        SELECT	year,
                                LEAD(TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP WITHOUT TIME ZONE, 1) OVER (
                                    ORDER BY year,month, day
                                ) - TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP WITHOUT TIME ZONE AS diff
                        FROM dim_date_times
                        ORDER BY year
                    ) SELECT	year,
                                AVG(diff) AS actual_time_taken
                    FROM sales_intervals
                    GROUP BY year
                    ORDER BY actual_time_taken DESC
                    LIMIT 5;
                    ''', conn)

print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))

print('\nProgram completed successfully')