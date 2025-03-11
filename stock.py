from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
import pandas as pd

# Snowflake Connection ID (Set in Airflow UI)
SNOWFLAKE_CONN_ID = "snowflake_conn"

# Alpha Vantage API Key (Stored in Airflow Variables)
API_KEY = Variable.get("alpha_vantage_api_key")

# Stock Symbol (Stored in Airflow UI)
STOCK_SYMBOL = Variable.get("stock_symbol") 

if not STOCK_SYMBOL:
    raise ValueError("Stock symbol is not set. Please add 'stock_symbol' in Airflow Variables.")

# Snowflake Table Details
TABLE_NAME = "RAW.STOCK_DATA"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG("stock_price_pipeline", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:

    @task()
    def create_table_if_not_exists():
        """Creates the Snowflake table if it doesn’t exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            symbol STRING,
            PRIMARY KEY (symbol, date)
        );
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snowflake_hook.run(create_table_sql)

    from datetime import datetime, timedelta

    @task()
    def extract_stock_data():
        """Fetches last 90 trading days of stock price data from Alpha Vantage"""
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={STOCK_SYMBOL}&apikey={API_KEY}&outputsize=compact"
        response = requests.get(url)
        data = response.json()
        
        if "Time Series (Daily)" not in data:
            raise ValueError("Error fetching stock data. Check API response.")
        
        # Convert to DataFrame
        df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")
        df.reset_index(inplace=True)
        df.rename(columns={
            "index": "date",
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume",
        }, inplace=True)
        df["symbol"] = STOCK_SYMBOL  # Use dynamic stock symbol

        # Convert date column to datetime
        df["date"] = pd.to_datetime(df["date"])
        
        # Sort DataFrame by date (newest first)
        df = df.sort_values(by="date", ascending=False)
        
        # Select last 90 trading days
        df = df.head(90)

        # Print the number of records fetched
        print(f"Total trading days extracted: {len(df)}")

        # Convert Timestamp to String (Fix serialization issue)
        df["date"] = df["date"].dt.strftime('%Y-%m-%d')

        # Convert to list of tuples for Snowflake insertion
        records = list(df.itertuples(index=False, name=None))

        return records  # Returns exactly 90 trading days of data


    @task()
    def full_refresh_snowflake(records):
        """Implements Full Refresh using SQL Transaction"""
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        if not records:
            raise ValueError("No records to insert into Snowflake")

        # Construct DELETE + INSERT SQL Transaction
        delete_sql = f"DELETE FROM {TABLE_NAME} WHERE symbol = '{STOCK_SYMBOL}'"
        insert_sql = f"""
            INSERT INTO {TABLE_NAME} (date, open, high, low, close, volume, symbol)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Execute in a single transaction
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN;")
                cur.execute(delete_sql)  # Delete existing records
                cur.executemany(insert_sql, records)  # Insert new records
                cur.execute("COMMIT;")  # Commit transaction

    # Define task dependencies
    create_table_task = create_table_if_not_exists()
    stock_data = extract_stock_data()
    refresh_task = full_refresh_snowflake(stock_data)

    create_table_task >> stock_data >> refresh_task  # Ensure order of execution