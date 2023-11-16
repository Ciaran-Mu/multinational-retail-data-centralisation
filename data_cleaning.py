import pandas as pd
from data_extraction import DataExtractor

class DataCleaning:
    '''
    This class is used to create an object for data cleaning purposes: taking in data in the form of pnadas dataframes and returning the same dataframes after cleaning

    No attributes to this class.
    '''

    @staticmethod
    def __check_luhn(card_number: int):
        '''
        This method checks if a card number is valid by checking it against Luhn's algorithm (used by major card providers).
        The final digit of a card number is a check digit calculated by Luhn's algorithm which is a sum of alternating 1 times and 2 times each digit.
        Where if the multiplication results in a number > 10 the two digits of that results are added together.
        And finally the check digit is calculated by taking the sum modulo 10 and subtracting it from 10.

        Args:
            card_number (int): the card number which is usually 8-19 digits but this method will work on any length.

        Returns:
            (bool): boolean result based on whether the final check digit matches the Luhn algorithm.
        '''
        sum = 0
        card_string = str(card_number)
        payload = card_string[-2::-1]
        for index, digit in enumerate(payload):
            digit = int(digit)
            if index % 2 == 1:
                sum += digit
            elif digit > 4:
                sum += digit * 2 - 9
            else:
                sum += digit * 2
        check_digit = 10 - sum % 10
        if card_string[-1] == str(check_digit):
            return True
        else:
            return False
    
    @staticmethod
    def clean_user_data(legacy_users_df: pd.DataFrame):
        '''
        This method cleans the dataframe of users details, removing null and junk records and correcting formats followed by converting to appropriate type.

        Args:
            legacy_users_df (DataFrame): pandas dataframe containing users details.

        Returns:
            legacy_users_df (DataFrame): pandas dataframe containing users details after cleaning.
        '''
        # Correct GGB error in country codes
        legacy_users_df['country_code'].replace('GGB', 'GB', inplace=True)
        # Drop the rows with NULL or rubbish data
        legacy_users_df.drop(legacy_users_df[legacy_users_df['country_code'].isin(('VSM4IZ4EL3', 'NULL', 'QVUW9JSKY3',
            '0CU6LW3NKB', 'PG8MOC0UZI', 'NTCGYW8LVC', 'FB13AKRI21',
            'OS2P9CMHR6', '5D74J6FPFJ', 'LZGTB0T5Z7', 'IM8MN1L9MJ',
            'RVRFD92E48', 'XKI9UXSCZ1', 'QREF9WLI2A', 'XPVCZE2L8B',
            '44YAIDY048'))].index, inplace=True)
        # Convert data_of_birth to datetime
        legacy_users_df['date_of_birth'] = pd.to_datetime(legacy_users_df['date_of_birth'], format='mixed')
        # Convert join_date to datetime
        legacy_users_df['join_date'] = pd.to_datetime(legacy_users_df['join_date'], format='mixed')
        # Tidy up addresses by swapping newline characters 
        legacy_users_df['address'].replace('\n', ', ', regex=True, inplace=True)
        # Set non phone numbers to none in the phone numbers column
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
        legacy_users_df.loc[~legacy_users_df['phone_number'].str.match(regex_expression), 'phone_number'] = None
        # Reformat phone numbers, first remove the (0) for options with +44(0) etc
        legacy_users_df['phone_number'].replace('\(0\)', '', regex=True, inplace=True)
        # Then swap country codes for zero and remove all non numeric characters
        legacy_users_df['phone_number'].replace({r'\+44': '0', r'\+49': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True, inplace=True)
        # TODO: consider reindexing dataframe
        return legacy_users_df

    @staticmethod
    def clean_card_data(card_details_df: pd.DataFrame):
        '''
        This method cleans the dataframe of users card details, removing null and junk records and correcting formats followed by converting to appropriate type.
        Card numbers are also checked for validity with Luhn's algorithm. As a significant portion of card numbers fail validation, a new column of (bool) representing pass/fail is created rather than deleting failed cards.

        Args:
            card_details_df (DataFrame): pandas dataframe containing users card details.

        Returns:
            card_details_df (DataFrame): pandas dataframe containing users card details after cleaning.
        '''
        # Drop any exact duplicates (10 found)
        card_details_df.drop_duplicates(inplace=True)
        # As expected these garbage card_provider entries are rows of garbage so can be dropped
        card_details_df.drop(card_details_df[~card_details_df['card_provider'].isin(('Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
            'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
            'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit'))].index, inplace=True)
        # Remove all the question marks preceding card_numbers
        card_details_df['card_number'].replace('\?*', '', regex=True, inplace=True)
        # The majority of card numbers do not pass luhn validation, so create a new column to reflect this rather than deleting data
        card_details_df.insert(1, 'valid_card_number', card_details_df['card_number'].apply(DataCleaning.__check_luhn))
        # Convert the card number to int
        card_details_df['card_number'] = pd.to_numeric(card_details_df['card_number'])
        # Convert the date payment confirmed to datetime
        card_details_df['date_payment_confirmed'] = pd.to_datetime(card_details_df['date_payment_confirmed'], format='mixed')
        # Convert expiry date to datetime (needed to be removed for SQL part)
        # card_details_df['expiry_date'] = pd.to_datetime(card_details_df['expiry_date'], format='%m/%y')
        # Reindex the dataframe
        card_details_df.reset_index(drop=True, inplace=True)
        return card_details_df
    
    @staticmethod
    def clean_stores_data(store_details_df: pd.DataFrame):
        '''
        This method cleans the dataframe of stores details, removing null and junk records and correcting formats followed by converting to appropriate type.

        Args:
            store_details_df (DataFrame): pandas dataframe containing stores details.

        Returns:
            store_details_df (DataFrame): pandas dataframe containing stores details after cleaning.
        '''
        # Remove the 10  rows full of NULL
        store_details_df.drop(store_details_df[store_details_df['lat'].isin(('13KJZ890JH', '2XE1OWOC23', 'NULL', 'OXVE5QR07O',
            'VKA5I8H32X', 'LACCWDI0SB', 'A3O5CBWAMD', 'UXMWDMX1LC'))].index, inplace=True)
        # # Remove any exact duplicates (none found)
        # legacy_store_df.drop_duplicates(inplace=True)
        # Convert longitude and latitude to float
        store_details_df['longitude'] = pd.to_numeric(store_details_df['longitude'], errors='coerce')
        store_details_df['latitude'] = pd.to_numeric(store_details_df['latitude'], errors='coerce')
        # Remove the rogue characters from the numbers in staff numbers
        store_details_df['staff_numbers'].replace('[a-zA-Z]', '', regex=True, inplace=True)
        # Convert staff_numbers to int
        store_details_df['staff_numbers'] = pd.to_numeric(store_details_df['staff_numbers'])
        # Drop the null column of 'lat'
        store_details_df.drop(columns='lat', inplace=True)
        # Convert opening date to datetime
        store_details_df['opening_date'] = pd.to_datetime(store_details_df['opening_date'], format='mixed')
        # Tidy up addresses by swapping newline characters 
        store_details_df['address'].replace('\n', ', ', regex=True, inplace=True)
        # Clean the continent column to remove extraneous ee
        store_details_df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'}, inplace=True)
        # Reindex the dataframe
        store_details_df.reset_index(drop=True, inplace=True)
        return store_details_df
    
    @staticmethod
    def __convert_product_weights(product_details_df: pd.DataFrame):
        '''
        This method converts the product weights from g, ml, oz all to kg and converts to float.
        Any records of weights written as a multiplication expression are equated before converting.

        Args:
            product_details_df (DataFrame): pandas dataframe containing product details.

        Returns:
            product_details_df (DataFrame): pandas dataframe containing product details after converting weights.
        '''
        # Take note of indexes of weights in g or ml that will need a factor of 0.001 conversion
        weight_g_indexes = product_details_df[product_details_df['weight'].str.contains('\d[g]|[m][l]', regex=True)].index
        # Take note of indexes of weights in oz that will need a factor of 0.0283 conversion
        weight_oz_indexes = product_details_df[product_details_df['weight'].str.contains('[o][z]', regex=True)].index
        # Remove the unit characters from weight
        product_details_df['weight'].replace('[m][l]|[k][g]|[g]|[o][z]', '', inplace=True, regex=True)
        # Sort out the rows that have a multiplication expression instead of a single number
        multi_indexes = product_details_df[product_details_df['weight'].str.contains('[x]', regex=True)].index
        multi_pairs = product_details_df.loc[multi_indexes]['weight'].str.partition(' x ')
        product_details_df.loc[multi_indexes, 'weight'] = pd.to_numeric(multi_pairs[0]) * pd.to_numeric(multi_pairs[2])
        # Remove any spurious spaces
        product_details_df['weight'].replace('\ ', '', inplace=True, regex=True)
        # Convert the weight to float
        product_details_df['weight'] = pd.to_numeric(product_details_df['weight'])
        # Make numerical conversions for g/ml to kg and oz to kg
        product_details_df.loc[weight_g_indexes, 'weight'] = product_details_df.loc[weight_g_indexes, 'weight'] * 0.001
        product_details_df.loc[weight_oz_indexes, 'weight'] = product_details_df.loc[weight_oz_indexes, 'weight'] * 0.0283

        return product_details_df
    
    @staticmethod
    def clean_products_data(product_details_df: pd.DataFrame):
        '''
        This method cleans the dataframe of product details, removing null and junk records and correcting formats followed by converting to appropriate type.

        Args:
            product_details_df (DataFrame): pandas dataframe containing product details

        Returns:
            product_details_df (DataFrame): pandas dataframe containing product details after cleaning
        '''
        # Drop the uselerss index column
        product_details_df.drop(columns='Unnamed: 0', inplace=True)
        # Drop the rows filled with all na
        product_details_df.dropna(axis=0, how='all', inplace=True)
        # Drop rows with junk data
        product_details_df.drop(product_details_df[~product_details_df['category'].isin(['toys-and-games', 'sports-and-leisure', 
                                                                'pets', 'homeware', 'health-and-beauty', 
                                                                'food-and-drink', 'diy'])].index, inplace=True)
        # Convert the weights all to kg
        product_details_df = DataCleaning.__convert_product_weights(product_details_df)
        # Convert the EAN to int
        product_details_df['EAN'] = pd.to_numeric(product_details_df['EAN'])
        # Drop the £ symbol from prices
        product_details_df['product_price'].replace('\£', '', inplace=True, regex=True)
        # Convert price to float
        product_details_df['product_price'] = pd.to_numeric(product_details_df['product_price'])
        # Convert date_added to datetime
        product_details_df['date_added'] = pd.to_datetime(product_details_df['date_added'], format='mixed')

        return product_details_df
    
    @staticmethod
    def clean_orders_data(orders_df: pd.DataFrame):
        '''
        This method cleans the dataframe of orders details, removing null and junk records and correcting formats followed by converting to appropriate type.

        Args:
            orders_df (DataFrame): pandas dataframe containing orders details

        Returns:
            orders_df (DataFrame): pandas dataframe containing orders details after cleaning
        '''
        # It seems the names are all present in the legacy-users table so they are just mixed up
        # Assume the users table is the valid name and so drop the first_name and last_name from orders tabel so that users can be referenced by uuid
        orders_df.drop(columns=['first_name', 'last_name'], inplace=True)
        # Drop the useless column
        orders_df.drop(columns='1', inplace=True)
        # Drop the level_0 column (around 1/6 values are different from index, the rest the same, so making an assumption that it was just an index at some point)
        orders_df.drop(columns='level_0', inplace=True)
        # The majority of card numbers do not pass luhn validation, so maybe create a new column to reflect this rather than deleting data
        orders_df.insert(3, 'valid_card_number', orders_df['card_number'].apply(DataCleaning.__check_luhn))

        return orders_df
    
    @staticmethod
    def clean_dates_data(dates_df: pd.DataFrame):
        '''
        This method cleans the dataframe of date details, removing null and junk records and correcting formats followed by converting to appropriate type.

        Args:
            dates_df (DataFrame): pandas dataframe containing date details

        Returns:
            dates_df (DataFrame): pandas dataframe containing date details after cleaning
        '''
        # Drop duplicates (14 found)
        dates_df.drop_duplicates()
        # Drop the rows filled with junk 
        dates_df.drop(dates_df[~dates_df['time_period'].isin(['Evening', 'Morning', 'Midday', 'Late_Hours'])].index, inplace=True)
        # Convert day month year to int
        dates_df['day'] = pd.to_numeric(dates_df['day'])
        dates_df['month'] = pd.to_numeric(dates_df['month'])
        dates_df['year'] = pd.to_numeric(dates_df['year'])

        return dates_df