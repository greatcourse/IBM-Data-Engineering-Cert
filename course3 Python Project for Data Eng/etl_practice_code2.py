import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Set Paths
tmpfile = "temp.tmp"  # file used to store all extracted data
logfile = "logfile.txt"  # all event logs will be stored in this file
targetfile = "transformed_data.csv"  # file where transformed data is stored

# CSV Extract Function
def extract_from_csv(file_to_process):
    return pd.read_csv(file_to_process)

# JSON Extract Function
def extract_from_json(file_to_process):
    return pd.read_json(file_to_process, lines=True)

# XML Extract Function
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()

    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text

        dataframe = pd.concat([dataframe, pd.DataFrame([{
            "car_model": car_model, 
            "year_of_manufacture": year_of_manufacture, 
            "price": price, 
            "fuel": fuel
        }])], ignore_index=True)

    return dataframe

# Extract Function
def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    # Process all CSV files
    for csvfile in glob.glob("dealership_data/*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # Process all JSON files
    for jsonfile in glob.glob("dealership_data/*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # Process all XML files
    for xmlfile in glob.glob("dealership_data/*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

# Transform Function: Round the price column to 2 decimal places
def transform(data):
    data['price'] = data['price'].round(2)
    return data

# Load Function: Save the transformed data to target file
def load(targetfile, data_to_load):
    try:
        data_to_load.to_csv(targetfile, index=False)  # Save without index
        print(f"Data successfully loaded into {targetfile}")
    except Exception as e:
        print(f"Error occurred while saving data: {e}")

# Logging Function
def log(message):
    timestamp_format = "%Y-%b-%d %H:%M:%S"  # Format: Year-Month-Day Hour:Minute:Second
    now = datetime.now()  # Get the current time
    timestamp = now.strftime(timestamp_format)  # Convert time to a formatted string
    
    with open("logfile.txt", "a") as f:
        f.write(f"{timestamp} - {message}\n")

# Running ETL Process
log("ETL Job Started")

# Extraction
log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

# Transformation
log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")

# Loading
log("Load phase Started")
load(targetfile, transformed_data)
log("Load phase Ended")

log("ETL Job Ended")
