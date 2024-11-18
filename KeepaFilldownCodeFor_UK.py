import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import time

# MySQL connection details for local database
local_db_config = {
    'database': '<username>',
    'host': '<username>',
    'user': '<username>',
    'password': '<username>',
    'connection_timeout': 600  # Increase timeout to 10 minutes
}

# Create an engine instance
engine = create_engine(f"mysql+mysqlconnector://{local_db_config['user']}:{local_db_config['password']}@{local_db_config['host']}/{local_db_config['database']}")
# Function to create tables if they do not exist
def create_tables():
    conn = mysql.connector.connect(**local_db_config)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS modified_price_history_uk (
        date DATE,
        asin VARCHAR(255),
        Price FLOAT
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS modified_category_rank_history_uk (
        date DATE,
        asin VARCHAR(255),
        category_type VARCHAR(255),
        category_name VARCHAR(255),
        category_rank FLOAT
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS modified_sub_category_rank_history_uk (
        date DATE,
        asin VARCHAR(255),
        category_type VARCHAR(255),
        sub_category_name VARCHAR(255),
        sub_category_rank FLOAT
    );""")
    conn.commit()
    cursor.close()
    conn.close()

 # Function to delete all existing values in the tables
def truncate_tables(tables):
    conn = mysql.connector.connect(**local_db_config)
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table};")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"All tables truncated: {tables}")

# Function to insert DataFrame into MySQL table with chunking
def insert_into_mysql(df, table_name, engine, chunk_size=1000):
    """
    Inserts data into MySQL table in chunks to avoid connection issues.
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, chunksize=chunk_size)
        print(f"Data inserted into table {table_name}")
    except Exception as e:
        print(f"Error while inserting data: {str(e)}")
        # Retry mechanism for inserting data
def insert_with_retries(df, table_name, engine, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            insert_into_mysql(df, table_name, engine)
            return
        except Exception as e:
            retries += 1
            print(f"Error on attempt {retries}: {e}")
            time.sleep(5)  # wait 5 seconds before retrying
            if retries == max_retries:
                print("Max retries reached. Insertion failed.")
                # Function to execute queries with OLAP workload
def execute_query_with_olap(query, db_config):
    conn = mysql.connector.connect(**db_config)
    try:
        cursor = conn.cursor()
        cursor.execute("SET workload = OLAP;")
        df = pd.read_sql(query, conn)
    finally:
        cursor.close()
        conn.close()
    return df

# MySQL connection details for remote database
remote_db_config = {
    'database': '<username>',
    'host': '<username>',
    'user': '<username>',
    'password': '<username>',
    'connection_timeout': 600  # Increase timeout to 10 minutes
}


# MySQL queries
category_query = """
SELECT
    DATE(category_rank_history_UK.`date`) AS `date`,
    CASE
        WHEN category_rank_history_UK.category IN (
            'Automotive',
            'Patio, Lawn & Garden',
            'Baby Products',
            'Tools & Home Improvement',
            'Sports & Outdoors',
            'Home & Kitchen',
            'Office Products'
        ) THEN 'Category'
        ELSE 'Sub_category'
    END AS category_type,
    category_rank_history_UK.category,
    category_rank_history_UK.asin,
    ROUND(MIN(category_rank_history_UK.`rank`)) AS avg_rank
    FROM
    category_rank_history_UK
GROUP BY
    DATE(category_rank_history_UK.`date`),
    category_type,
    category_rank_history_UK.category,
    category_rank_history_UK.asin;
"""

price_query = """
SELECT
    DATE(new_price_history_UK.`date`) AS `date`,
    new_price_history_UK.asin,
    ROUND(MAX(new_price_history_UK.price), 2) AS Price
FROM
    new_price_history_UK
GROUP BY
    DATE(new_price_history_UK.`date`),
    new_price_history_UK.asin;
"""
# Fetch data with OLAP workload
category_df = execute_query_with_olap(category_query, remote_db_config)
price_df = execute_query_with_olap(price_query, remote_db_config)

# Convert date columns to datetime
category_df['date'] = pd.to_datetime(category_df['date'])
price_df['date'] = pd.to_datetime(price_df['date'])

# Create a date range that spans from the minimum date in the datasets to today's date
start_date = min(category_df['date'].min(), price_df['date'].min())
end_date = pd.to_datetime('today')
date_range = pd.date_range(start=start_date, end=end_date)

# Create a new DataFrame with all dates for each ASIN
asins = category_df['asin'].unique()
# Function to fill down missing values
def fill_down(df, date_range, asins):
    date_asin_combinations = pd.MultiIndex.from_product([date_range, asins], names=['date', 'asin'])
    df_full = pd.DataFrame(index=date_asin_combinations).reset_index()
    df_full = pd.merge(df_full, df, on=['date', 'asin'], how='left')
    df_full = df_full.sort_values(by=['asin', 'date'])
    df_full['Price'] = df_full.groupby('asin')['Price'].fillna(method='ffill')
    return df_full

# Function to fill down missing values for category and subcategory data
def fill_down_category(df, date_range, asins, category_type):
    date_asin_combinations = pd.MultiIndex.from_product([date_range, asins], names=['date', 'asin'])
    df_full = pd.DataFrame(index=date_asin_combinations).reset_index()
    df_full['category_type'] = category_type
    df_full = pd.merge(df_full, df, on=['date', 'asin', 'category_type'], how='left')
    df_full = df_full.sort_values(by=['asin', 'date'])
    df_full['avg_rank'] = df_full.groupby('asin')['avg_rank'].fillna(method='ffill')
    df_full['category'] = df_full.groupby('asin')['category'].fillna(method='ffill')
    return df_full

# Apply fill down function to the price data
filled_price_df = fill_down(price_df, date_range, asins)

# Clean up the final price DataFrame
final_price_df = filled_price_df[['date', 'asin', 'Price']]

# Separate data into two tables
category_df_only = category_df[category_df['category_type'] == 'Category']
sub_category_df_only = category_df[category_df['category_type'] == 'Sub_category']

# Apply fill down function to both tables
filled_category_df = fill_down_category(category_df_only, date_range, asins, 'Category')
filled_sub_category_df = fill_down_category(sub_category_df_only, date_range, asins, 'Sub_category')

# Rename columns to prepare for merge
filled_category_df = filled_category_df.rename(columns={'avg_rank': 'category_rank', 'category': 'category_name'})
filled_sub_category_df = filled_sub_category_df.rename(columns={'avg_rank': 'sub_category_rank', 'category': 'sub_category_name'})

# Clean up the final DataFrames
final_category_df = filled_category_df[['date', 'asin', 'category_type', 'category_name', 'category_rank']]
final_sub_category_df = filled_sub_category_df[['date', 'asin', 'category_type', 'sub_category_name', 'sub_category_rank']]

# Reset index for clean output
final_category_df = final_category_df.reset_index(drop=True)
final_sub_category_df = final_sub_category_df.reset_index(drop=True)
final_price_df = final_price_df.reset_index(drop=True)

# Create tables if they do not exist
create_tables()

# Truncate tables before inserting new data
tables_to_truncate = ['modified_price_history_uk', 'modified_category_rank_history_uk', 'modified_sub_category_rank_history_uk']
truncate_tables(tables_to_truncate)


# Insert data into local MySQL database using retry mechanism
insert_with_retries(final_price_df, 'modified_price_history_uk', engine)
insert_with_retries(final_category_df, 'modified_category_rank_history_uk', engine)
insert_with_retries(final_sub_category_df, 'modified_sub_category_rank_history_uk', engine)

# Save DataFrames to CSV files
final_price_df.to_csv('filled_price_data.csv', index=False)
final_category_df.to_csv('category_data.csv', index=False)
final_sub_category_df.to_csv('sub_category_data.csv', index=False)

# Output data for verification
print("Filled Price DataFrame:")
print(final_price_df.head())
print("Category DataFrame:")
print(final_category_df.head())
print("Sub_category DataFrame:")
print(final_sub_category_df.head())
