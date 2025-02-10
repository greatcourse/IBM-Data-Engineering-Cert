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
    dataframe = pd.read_csv(file_to_process)
    return dataframe


# JSON Extract Function
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


# XML Extract Function
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat(
            [dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])],
            ignore_index=True,
        )
    return dataframe


# Extract Function
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])  # create an empty data frame

    # process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data


# Transform Function
def transform(data):
    # Convert height (in inches) to meters
    data["height"] = round(data.height * 0.0254, 2)

    # Convert weight (in pounds) to kilograms
    data["weight"] = round(data.weight * 0.45359237, 2)

    return data


# Load Function
def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile, index=False)


# Logging Function
def log(message):
    timestamp_format = "%Y-%b-%d-%H:%M:%S"  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(logfile, "a") as f:
        f.write(timestamp + "," + message + "\n")


# Running ETL Process
log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

log("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)
log("Transform phase Ended")

log("Load phase Started")
load(targetfile, transformed_data)
log("Load phase Ended")

log("ETL Job Ended")

