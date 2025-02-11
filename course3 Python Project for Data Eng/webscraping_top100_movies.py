# Importing Libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# Initialization of known entities
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_25'
csv_path = '/home/project/top_25_films.csv'

df = pd.DataFrame(columns=["Film", "Year", "Rotten Tomatoes' Top 100"])
count = 0

# Loading the webpage for Webscraping
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# Scraping required information
tables = data.find_all('tbody')

if tables:  # Ensure that a table exists
    rows = tables[0].find_all('tr')

    for row in rows:
        if count < 25:
            cols = row.find_all('td')
            if len(cols) >= 4:  # Ensure it has enough columns
                try:
                    film = cols[1].get_text(strip=True)
                    year = int(cols[2].get_text(strip=True))  # Convert to int
                    rt_rank = cols[3].get_text(strip=True)

                    if year >= 2000:  # Filter films from the 2000s
                        df1 = pd.DataFrame({"Film": [film], "Year": [year], "Rotten Tomatoes' Top 100": [rt_rank]})
                        df = pd.concat([df, df1], ignore_index=True)
                        count += 1
                except ValueError:
                    continue  # Skip rows where year extraction fails

# Print the filtered results
print(df)

# Save to CSV
df.to_csv(csv_path, index=False)

# Save to Database
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
