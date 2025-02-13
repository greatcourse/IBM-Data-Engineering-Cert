import sqlite3
import pandas as pd

# Creating Database
conn = sqlite3.connect('STAFF.db')
# Create and Load the table
table_name = 'Departments'
attribute_list = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']
# Reading the CSV file
file_path = '/home/project/Departments.csv'
df = pd.read_csv(file_path, names = attribute_list)
# Loading the data to a table
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')
# Running basic queries on data
# Viewing all the data in the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
#Viewing only FNAME column of data.
query_statement = f"SELECT DEP_NAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
#Viewing the total number of entries in the table.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
# Now try appending some data to the table. Consider the following.
#a. Assume the DEPT_ID is 9.
#b. Assume the DEP_NAME, is Quality Assurance.
#c. Assume the lMANAGER_ID, 30010.
#d. Assume the LOC_ID, L0010.

data_dict = {'DEPT_ID' : [9],
            'DEP_NAME' : ['Quality Assurance'],
            'MANAGER_ID' : ['30010'],
            'LOC_ID' : ['L0010']}
data_append = pd.DataFrame(data_dict)
# append the data to the Departments table.
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

# Query 4: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
# Close the connection
conn.close()