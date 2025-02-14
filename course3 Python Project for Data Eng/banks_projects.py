import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
import requests
from datetime import datetime

# Define file paths and table attributes
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_name = 'Largest_banks'
base_dir = '5 - Final Project'
csv_name = 'Largest_banks_data.csv'
log_file = 'code_log.txt'
exchange_rate_filename = 'exchange_rate.csv'
attribute_list_extract = ['Country', 'MC_USD_Billion']
attribute_list_final = ['Country', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']

# File paths
csv_path = os.path.join(base_dir, 'output', csv_name)
db_path = os.path.join(base_dir, 'database', db_name)
log_path = os.path.join(base_dir, 'log', log_file)
data_dir = os.path.join(base_dir, 'data')
exchange_rate_path = os.path.join(data_dir, exchange_rate_filename)

# Ensure required directories exist
for path in [os.path.dirname(csv_path), os.path.dirname(db_path), os.path.dirname(log_path), data_dir]:
    os.makedirs(path, exist_ok=True)

# Import exchange rate data
exchange_rate = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv")
exchange_rate.to_csv(exchange_rate_path, index=False)

# Log_progress function
def log_progress(message):
    """Logs the execution progress into a log file."""
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    timestamp = datetime.now().strftime(timestamp_format)
    with open(log_path, "a") as f:
        f.write(f"{timestamp}, {message}\n")

# Extract function
def extract(url):
    """Extracts bank market capitalization data from Wikipedia and returns it as a DataFrame."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = BeautifulSoup(response.text, 'html.parser')
        tables = data.find_all('tbody')
        rows = tables[0].find_all('tr')
        
        df = pd.DataFrame(columns=attribute_list_extract)
        count = 0
        for row in rows:
            if count < 10:
                col = row.find_all('td')
                if len(col) != 0:
                    try:
                        country = col[1].find_all('a')[1].get('title')
                        market_cap = float(col[2].contents[0].strip().replace(',', ''))
                        df = pd.concat([df, pd.DataFrame({"Country": [country], "MC_USD_Billion": [market_cap]})], ignore_index=True)
                        count += 1
                    except (IndexError, ValueError) as e:
                        log_progress(f"Skipping row due to extraction error: {e}")
        return df
    except Exception as e:
        log_progress(f"Extract Error: {e}")
        return pd.DataFrame(columns=attribute_list_extract)

# Transform function
def transform(df, new_columns):
    """Transforms the extracted data by adding currency conversion columns."""
    try:
        if df.empty:
            log_progress("No data to transform")
            return df

        exchange_rates = pd.read_csv(exchange_rate_path).set_index('Currency')
        df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['GBP', 'Rate'], 2)
        df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['EUR', 'Rate'], 2)
        df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['INR', 'Rate'], 2)
        
        return df
    except Exception as e:
        log_progress(f"Transform Error: {e}")
        return df

# Load functions
def load_to_csv(df, csv_path):
    """Saves the final DataFrame as a CSV file."""
    try:
        df.to_csv(csv_path, index=False)
    except Exception as e:
        log_progress(f"CSV Load Error: {e}")

def load_to_db(df, db_path, table_name):
    """Loads the final DataFrame into an SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
    except Exception as e:
        log_progress(f"Database Load Error: {e}")

# Query execution function
def execute_query(db_path, query):
    """Executes a SQL query on the database and prints the result."""
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        print(df.to_string(index=False))
        print("\n")
        return df
    except Exception as e:
        log_progress(f"Query Execution Error: {e}")
        return None

# Run ETL Process
log_progress("ETL Job Started")
df_extracted = extract(url)
df_transformed = transform(df_extracted, attribute_list_final)
load_to_csv(df_transformed, csv_path)
load_to_db(df_transformed, db_path, table_name)

# Print 5th largest bank market capitalization in EUR
if 'MC_EUR_Billion' in df_transformed.columns and len(df_transformed) > 4:
    print("Market Capitalization of the 5th largest bank in EUR:", df_transformed['MC_EUR_Billion'][4])
else:
    print("DataFrame does not contain enough rows or the column is missing.")

log_progress("Bank Job Finished")
