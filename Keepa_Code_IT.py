import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import logging
import sys
from sqlalchemy import create_engine
from asins_our_uk_and_it import asins

# Set up logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

suffix = "our_asins"

def convert_unix_time(value):
    return datetime.utcfromtimestamp((value + 21564000) * 60).strftime('%Y-%m-%d %H:%M:%S')

def fetch_keepa_data(asins, access_key):
    domain_id = 8  # assuming Amazon CA
    all_coupons_data = []
    all_lightning_deals_data = []
    all_new_price_history_data = []
    all_sales_history_data = []
    all_sales_rank_data = []
    all_category_rank_data = []

    for asin in asins:
        url = f'https://api.keepa.com/product?key={access_key}&domain={domain_id}&asin={asin}'
        response = requests.get(url)
        if response.status_code == 200:
            logging.info(f"Successfully fetched data for ASIN: {asin}")
            data = response.json()
            product = data.get('products', [{}])[0]
            try:
                # Coupon Data
                if product.get('productType') == 0 and product.get('couponHistory'):
                    coupon_data = [
                        {
                            'asin': asin,
                            'date': convert_unix_time(product['couponHistory'][i]),
                            'one_time_coupon': (
                                f"{abs(product['couponHistory'][i+1])}%" if product['couponHistory'][i+1] < 0
                                else f"${product['couponHistory'][i+1] / 100:.2f}"
                            ) if product['couponHistory'][i+1] != 0 else None,
                            'subscribe_and_save_coupon': (
                                f"{abs(product['couponHistory'][i+2])}%" if product['couponHistory'][i+2] < 0
                                else f"${product['couponHistory'][i+2] / 100:.2f}"
                            ) if product['couponHistory'][i+2] != 0 else None
                        }
                        for i in range(0, len(product['couponHistory']), 3)
                    ]
                    all_coupons_data.extend(coupon_data)

                # Lightning Deal Data
                if product.get('csv') and product['csv'][8] is not None:
                    lightning_deal_data = [
                        {
                            'asin': asin,
                            'date': convert_unix_time(product['csv'][8][i]),
                            'price': product['csv'][8][i+1] / 100
                        }
                        for i in range(0, len(product['csv'][8]), 2)
                    ]
                    all_lightning_deals_data.extend(lightning_deal_data)

                # New Price History Data
                if product.get('csv') and product['csv'][1] is not None:
                    new_price_history_data = [
                        {
                            'asin': asin,
                            'date': convert_unix_time(product['csv'][1][i]),
                            'price': product['csv'][1][i+1] / 100
                        }
                        for i in range(0, len(product['csv'][1]), 2)
                    ]
                    all_new_price_history_data.extend(new_price_history_data)

                # Sales Data
                if product.get('csv') and product['csv'][0] is not None:
                    sales_data = [
                        {
                            'asin': asin,
                            'date': convert_unix_time(product['csv'][0][i]),
                            'price': product['csv'][0][i+1] / 100 if product['csv'][0][i+1] != -1 else -1
                        }
                        for i in range(0, len(product['csv'][0]), 2)
                    ]
                    all_sales_history_data.extend(sales_data)

                # Sales Rank Data
                if product.get('csv') and product['csv'][3] is not None:
                    sales_rank_data = [
                        {
                            'asin': asin,
                            'date': convert_unix_time(product['csv'][3][i]),
                            'rank': product['csv'][3][i+1]
                        }
                        for i in range(0, len(product['csv'][3]), 2)
                    ]
                    all_sales_rank_data.extend(sales_rank_data)

                # Category Rank Data
                if product.get('categoryTree'):
                    for category in product['categoryTree']:
                        category_id = str(category['catId'])
                        if category_id in product.get('salesRanks', {}):
                            category_rank_data = [
                                {
                                    'asin': asin,
                                    'category': category['name'],
                                    'date': convert_unix_time(product['salesRanks'][category_id][i]),
                                    'rank': product['salesRanks'][category_id][i+1]
                                }
                                for i in range(0, len(product['salesRanks'][category_id]), 2)
                            ]
                            all_category_rank_data.extend(category_rank_data)

            except Exception as e:
                logging.error(f"Error processing ASIN {asin}: {e}")
        else:
            logging.error(f"Failed to fetch data for ASIN: {asin}, Status Code: {response.status_code}")

    # Convert lists to DataFrames
    def process_data_to_df(data_list):
        return pd.DataFrame(data_list) if data_list else pd.DataFrame()

    return {
        "COUPON_HISTORY": process_data_to_df(all_coupons_data),
        "LIGHTNING_DEAL": process_data_to_df(all_lightning_deals_data),
        "NEW_PRICE_HISTORY": process_data_to_df(all_new_price_history_data),
        "SALES": process_data_to_df(all_sales_history_data),
        "SALES_RANK": process_data_to_df(all_sales_rank_data),
        "CATEGORY_RANK": process_data_to_df(all_category_rank_data)
    }

def safe_filter_and_convert(df, start_date, end_date, key_name):
    """
    Safely filter and convert DataFrame for a specific key in the data dictionary.
    Logs warnings and skips processing for empty or malformed DataFrames.
    """
    if df.empty:
        logging.warning(f"DataFrame for '{key_name}' is empty. Skipping filtering.")
        return df

    if 'date' not in df.columns:
        logging.warning(f"'date' column not found in DataFrame for '{key_name}'. Skipping filtering.")
        return df

    logging.info(f"Filtering DataFrame for '{key_name}' from {start_date} to {end_date}.")
    df['date'] = pd.to_datetime(df['date'])  # Ensure date column is in datetime format
    return df[(df['date'] >= start_date) & (df['date'] <= end_date)]

def update_database(engine, table_name, df):
    """
    Update the database with the given DataFrame.
    If the table exists, delete its contents before inserting.
    If the table does not exist, create it and insert the data.
    """
    if df.empty:
        logging.warning(f"No data to update for table {table_name}.")
        return

    with engine.begin() as conn:
        logging.info(f"Inserting data into table {table_name}.")
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f"Table {table_name} updated with {len(df)} records.")

# Database credentials
database = "<username>"
host = "<username>"
username = "<username>"
password = "<username>"
database_url = f"mysql+mysqlconnector://{username}:{password}@{host}/{database}"

# Create a SQLAlchemy engine
engine = create_engine(database_url)

# Fetch and process Keepa data
access_key = '<username>'
logging.info("Starting to fetch Keepa data")
data = fetch_keepa_data(asins, access_key)
logging.info("Keepa data fetched and processed")

# Get the current year range
start_date = datetime(datetime.now().year, 1, 1)
end_date = datetime.now()

# Filter data for the current year
filtered_category_rank_df = safe_filter_and_convert(data["CATEGORY_RANK"], start_date, end_date, "CATEGORY_RANK")
filtered_coupon_history_df = safe_filter_and_convert(data["COUPON_HISTORY"], start_date, end_date, "COUPON_HISTORY")
filtered_lightning_deal_df = safe_filter_and_convert(data["LIGHTNING_DEAL"], start_date, end_date, "LIGHTNING_DEAL")
filtered_new_price_history_df = safe_filter_and_convert(data["NEW_PRICE_HISTORY"], start_date, end_date, "NEW_PRICE_HISTORY")
filtered_sales_history_df = safe_filter_and_convert(data["SALES"], start_date, end_date, "SALES")
filtered_sales_rank_df = safe_filter_and_convert(data["SALES_RANK"], start_date, end_date, "SALES_RANK")

# Update the database with the filtered data
update_database(engine, 'category_rank_history_IT', filtered_category_rank_df)
update_database(engine, 'coupon_history_IT', filtered_coupon_history_df)
update_database(engine, 'lightning_deal_IT', filtered_lightning_deal_df)
update_database(engine, 'new_price_history_IT', filtered_new_price_history_df)
update_database(engine, 'sales_history_IT', filtered_sales_history_df)
update_database(engine, 'sales_rank_IT', filtered_sales_rank_df)

logging.info("Database update completed")
