from database_utils import DatabaseConnector
from tabulate import tabulate

import pandas as pd


# Decorator to print the returned SQL queries (pandas DataFrame format) to the terminal 
def query_printer(func):
    def wrapper(arg):
        result = func(arg)
        print(tabulate(result, headers='keys', tablefmt='psql', showindex=False, floatfmt='.2f'))
    return wrapper

# Decorator to ask the user if they wish to continue after viewing the current query results
def user_continue(func):
    def wrapper(arg):
        func(arg)
        input('\nContinue to next query? Press Enter')
    return wrapper

class DataQuerier:

    @staticmethod
    @user_continue
    @query_printer
    def Task_1_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
                            SELECT  country_code,
                                    COUNT(store_code) AS total_no_stores
                            FROM dim_store_details
                            GROUP BY country_code
                            ORDER BY total_no_stores DESC;
                            ''', conn)
    
    @staticmethod
    @user_continue
    @query_printer
    def Task_2_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
                            SELECT  locality,
                                    COUNT(store_code) AS total_no_stores
                            FROM dim_store_details
                            GROUP BY locality
                            ORDER BY total_no_stores DESC
                            LIMIT 7;
                            ''', conn)

    @staticmethod
    @user_continue
    @query_printer
    def Task_3_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
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

    @staticmethod
    @user_continue
    @query_printer
    def Task_4_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
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

    @staticmethod
    @user_continue
    @query_printer
    def Task_5_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
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
    
    @staticmethod
    @user_continue
    @query_printer
    def Task_6_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
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
    
    @staticmethod
    @user_continue
    @query_printer
    def Task_6_query_variation(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
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

    @staticmethod
    @user_continue
    @query_printer
    def Task_7_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
                    SELECT	SUM(staff_numbers) AS total_staff_numbers,
                            country_code
                    FROM dim_store_details
                    GROUP BY country_code
                    ORDER BY total_staff_numbers DESC;
                    ''', conn)

    @staticmethod
    @user_continue
    @query_printer
    def Task_8_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
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

    @staticmethod
    @query_printer
    def Task_9_query(db_connector: DatabaseConnector):
        engine = db_connector.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_query('''
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