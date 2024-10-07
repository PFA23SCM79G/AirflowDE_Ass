import requests
import pandas as pd
import kaggle
import os

# Dictionary to keep track of the latest date loaded for each dataset
last_loaded = {'first_file': None, 'second_file': None}

# Global DataFrames
gdp_data = pd.DataFrame()
df1 = pd.DataFrame()
df2 = pd.DataFrame()


def download_world_bank_data():
    global gdp_data
    # API endpoint for World Bank GDP per capita data
    url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD"

    # Parameters for the request
    params = {
        "format": "json",
        "per_page": "5000",  # Fetches a large number of rows per request
    }

    # Send the GET request to the World Bank API
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response to JSON format
        data = response.json()

        # The first item contains metadata, the second contains actual data
        gdp_data = pd.DataFrame(data[1])

        # Filter relevant columns and clean the data
        gdp_data_clean = gdp_data[['countryiso3code', 'country', 'date', 'value']]
        gdp_data_clean['date'] = pd.to_numeric(gdp_data_clean['date'], errors='coerce')

        # Save the data to a CSV file
        gdp_data_clean.to_csv('gdp_per_capita_world_bank.csv', index=False)

        print("File saved as 'gdp_per_capita_world_bank.csv'.")
    else:
        print(f"Failed to fetch data: {response.status_code}")

def download_kaggle_dataset():
    kaggle.api.authenticate()
    print(kaggle.api.dataset_list_files('gpreda/covid-world-vaccination-progress').files)
    kaggle.api.dataset_download_files('gpreda/covid-world-vaccination-progress', path='.', unzip=True)

def load_gdp_data(file_path: str):
    # Load the GDP data from the CSV file
    global gdp_data
    gdp_data = pd.read_csv(file_path)

    # Perform any data cleaning or transformations here (if necessary)
    # For example, ensure certain columns are numeric, remove NaN values, etc.

    #print(gdp_data.head())  # Preview the loaded data (you can remove this in production)

    return gdp_data

def load_covid_data(first_file_path: str, second_file_path: str):
    
    global last_loaded, df1, df2

    # Load the first CSV (Afghanistan vaccination data)
    df1 = pd.read_csv(first_file_path, parse_dates=['date'], dayfirst=True)  # Ensuring 'date' is in datetime format

    # Load the second CSV (Argentina vaccination data)
    df2 = pd.read_csv(second_file_path, parse_dates=['date'], dayfirst=True)

    # Incremental load logic for the first file
    if last_loaded['first_file'] is not None:
        df1_new = df1[df1['date'] > last_loaded['first_file']]
    else:
        df1_new = df1

    # Incremental load logic for the second file
    if last_loaded['second_file'] is not None:
        df2_new = df2[df2['date'] > last_loaded['second_file']]
    else:
        df2_new = df2

    # Update the tracking for the last loaded date
    if not df1_new.empty:
        last_loaded['first_file'] = df1_new['date'].max()

    if not df2_new.empty:
        last_loaded['second_file'] = df2_new['date'].max()

    # Return the new data
    return df1_new, df2_new

# Example usage for World Bank data
download_world_bank_data()

# Example usage for Kaggle dataset download
download_kaggle_dataset()

# Example usage for loading COVID data incrementally
first_file_path = 'D:\DEprojects\Assignment-Airflow\country_vaccinations.csv'  # Replace with actual path
second_file_path = 'D:\DEprojects\Assignment-Airflow\country_vaccinations_by_manufacturer.csv'  # Replace with actual path

df1_new, df2_new = load_covid_data(first_file_path, second_file_path)

#print("New records from the first file:")
#print(df1_new)

#print("New records from the second file:")
#print(df2_new)

# Example usage for GDP data loading
load_gdp_data('D:\DEprojects\Assignment-Airflow\gdp_per_capita_world_bank.csv')
