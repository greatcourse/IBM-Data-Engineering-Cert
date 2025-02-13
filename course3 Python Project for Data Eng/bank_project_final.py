import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os

# Define file paths
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
csv_name = 'Largest_banks_data.csv'
table_name = 'Largest_Banks'
attribute_list_extract = ['Country', 'MC_USD_Billion']
attribute_list_final = ['Country', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']

# File paths
base_dir = '5 - Final Project'
csv_path = os.path.join(base_dir, 'output', csv_name)
db_path = os.path.join(base_dir, 'database', db_name)
log_path = os.path.join(base_dir, 'log', "code_log.txt")
exchange_rate_path = os.path.join(base_dir, 'data', 'exchange_rate.csv')

# Ensure required directories exist
for path in [os.path.dirname(csv_path), os.path.dirname(db_path), os.path.dirname(log_path)]:
    os.makedirs(path, exist_ok=True)

def log_progress(message): 
    """Logs the execution progress into a log file."""
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-Month-Day-Hour-Minute-Second
    timestamp = datetime.now().strftime(timestamp_format)

    with open(log_path, "a") as f: 
        f.write(f"{timestamp}, {message}\n")

def extract(url):
    """Extracts bank market capitalization data from Wikipedia and returns it as a DataFrame."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP issues
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
                        market_cap = float(col[2].contents[0].strip().replace(',', ''))  # Handle commas in numbers
                        df = pd.concat([df, pd.DataFrame({"Country": [country], "MC_USD_Billion": [market_cap]})], ignore_index=True)
                        count += 1
                    except (IndexError, ValueError) as e:
                        log_progress(f"Skipping row due to extraction error: {e}")

        return df
    except Exception as e:
        log_progress(f"Extract Error: {e}")
        return pd.DataFrame(columns=attribute_list_extract)

def transform(df, new_columns):
    """Transforms the extracted data by adding currency conversion columns."""
    try:
        if df.empty:
            log_progress("No data to transform")
            return df

        # Ensure required columns are present
        for col in new_columns:
            if col not in df.columns:
                df[col] = None

        # Read exchange rates
        exchange_rates = pd.read_csv(exchange_rate_path).set_index('Currency')

        # Convert USD to other currencies
        df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['GBP', 'Rate'], 2)
        df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['EUR', 'Rate'], 2)
        df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['INR', 'Rate'], 2)

        return df
    except Exception as e:
        log_progress(f"Transform Error: {e}")
        return df

def load_to_csv(csv_path, df):
    """Saves the final DataFrame as a CSV file."""
    try:
        df.to_csv(csv_path, index=False)
    except Exception as e:
        log_progress(f"CSV Load Error: {e}")

def load_to_db(db_path, df, table_name):
    """Loads the final DataFrame into an SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
    except Exception as e:
        log_progress(f"Database Load Error: {e}")

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

# Extraction
log_progress("Extract phase Started")
df_extracted = extract(url)
log_progress("Extract phase Ended")

# Transformation
log_progress("Transform phase Started")
df_transformed = transform(df_extracted, attribute_list_final)
log_progress("Transform phase Ended")

# Loading
log_progress("Load phase Started")
load_to_csv(csv_path, df_transformed)
load_to_db(db_path, df_transformed, table_name)
log_progress("Load phase Ended")

# Queries
log_progress("Query for the entire table Started")
query = "SELECT * FROM Largest_Banks"
execute_query(db_path, query)
log_progress("Query for the entire table Ended")

log_progress("Query for the average MC_GBP_Billion from all the banks Started")
query = "SELECT AVG(MC_GBP_Billion) FROM Largest_Banks"
execute_query(db_path, query)
log_progress("Query for the average MC_GBP_Billion from all the banks Ended")

log_progress("Query for the top 5 banks Started")
query = "SELECT * FROM Largest_Banks ORDER BY MC_USD_Billion DESC LIMIT 5"
execute_query(db_path, query)
log_progress("Query for the top 5 banks Ended")

log_progress("ETL Job Finished")
