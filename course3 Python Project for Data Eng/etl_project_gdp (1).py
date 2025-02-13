import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GD'
attribute_list = ['Country', 'GDP_USD_Billion']

json_path = 'Practice Project/output/Countries_by_GDP.json'
database_path = 'Practice Project/database/World_Economies.db'

log_file = "etl_project_log.txt"
log_path = "Practice Project/logs/etl_project_log.txt"

def log_progress(message): 
    log_file_path = log_path
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    
    with open(log_file_path,"a") as f: 
        f.write(timestamp + ',' + message + '\n')

df = pd.DataFrame(columns=["Country", "GDP_USD_Billion"])
# Website contains 213 entries
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
# 3 table in the page!
rows = tables[2].find_all('tr')

log_progress("Web Scraping Job Started")
for row in rows[3:]:  # Start iterating from the 3 row
    if count<214:
        col = row.find_all('td')
        if len(col)!=0:
            country_col = col[0].find('a')  # Find the 'a' tag within the 'td'
            if country_col:  # Check if 'a' tag exists
                country_name = country_col.text  # Extract the text from 'a' tag
            else:
                country_name = col[0].text  # If 'a' tag doesn't exist, get the text from 'td' tag
            
            gdp_value = col[2].contents[0].replace(',', '')
            if gdp_value == '-' or gdp_value == '—':
                gdp_in_billion = None
            else:
                # Convert GDP from string to float, divide by 1,000 to convert from million to billion, and round to 2 decimal places
                gdp_in_billion = round(float(gdp_value) / 1000, 2)
            
            data_dict = {"Country": country_name,
                         "GDP_USD_Billion": gdp_in_billion,}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            log_progress(f"Index[{count}] done") 
            count+=1
    else:
        break
log_progress(f'Web Scraping Job Done')

# Saving to JSON
df.to_json(json_path, orient='records', lines=True)
log_progress(f'Saving JSON')

# Saving info in Database
log_progress(f'Connecting to database')
conn = sqlite3.connect(database_path)

log_progress(f'Saving df to SQL')
df.to_sql(table_name, conn, if_exists='replace', index=False)

query = "SELECT * FROM {} WHERE GDP_USD_Billion > 100".format(table_name)
df2 = pd.read_sql_query(query, conn)
log_progress(f'Printing Query Result to Terminal')
print(df2)

conn.close()