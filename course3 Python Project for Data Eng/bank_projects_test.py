import requests
from bs4 import BeautifulSoup

# URL of the archived Wikipedia page
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

# Send an HTTP request and get the response
response = requests.get(url)
response.raise_for_status()  # Ensure the request was successful

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.text, "html.parser")

# Find all tables in the webpage
tables = soup.find_all("table", {"class": "wikitable"})

# Print the number of tables found (for debugging)
print(f"Total tables found: {len(tables)}")

# Identify the correct table under "By market capitalization"
desired_table = tables[1]  # Assuming the second table contains the required data

# Print the extracted HTML of the table
table_html = str(desired_table)
print(table_html)

# Save the extracted HTML table to a local file
with open("extracted_table.html", "w", encoding="utf-8") as file:
    file.write(table_html)

print("HTML table extracted and saved as extracted_table.html")