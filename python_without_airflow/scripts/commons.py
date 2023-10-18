import requests
import csv
from datetime import datetime
import os
import json


def fetch_data(url: str) -> json:
    """
    This function fetchs the data from the given API
    Args:
        url: API from where the data is fetched
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as e:
        print(f"An error occurred: {e}")
    return None


def data_transformation(data: dict, folder_path: str, filename: str, 
                main_key: str) -> None:
    """
    This Function does data tranformation for csv data. Its Unnest the inner dictionary 
    and writes main key as an separate column corresponding to its inner value and writes 
    to csv.
    Args:
        data: Data that is fetched from the API.
        folder_path: folder where the csv is stored.
        filename: Name of the csv file.
        main_key: It is the key of dictionary which is converted to sperate column
                  and append with it sub dictionary.
    """
    if data:
        # Create the folder path if it doesn't exist
        os.makedirs(folder_path, exist_ok=True)
        # Combine the folder path and filename to get the full file path
        file_path = os.path.join(folder_path, filename)
        headers = set()
        for d in data.values():
            headers.update(d.keys())
        # Create a list of dictionaries with restructured data
        csv_data = []
        for key, inner_dict in data.items():
            row = {main_key: key}
            row.update(inner_dict)
            csv_data.append(row)
        fieldnames = [main_key] + sorted(headers)
        write_to_csv(file_path, fieldnames, csv_data)


def write_to_csv(file_path: str, fieldnames: list, csv_data: list) -> None:
    """ 
    This function writes the content to csv file.
    Args:
        file_path: path to the csv file.
        fieldnames: Names of the  header.
        csv_data: data that is written in csv file.
    """
    with open(file_path, 'w', newline='') as csvfile:
        # Extract headers from the inner dictionaries
        # Define the fieldnames for the CSV
        # Write the data to the CSV file
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_data)


def date_partitioning() -> None:
    """
    This function creates a dicrectory based on the date partitioning
    """
    current_date = datetime.now()
    year = str(current_date.year)
    month = current_date.strftime('%m')
    day = current_date.strftime('%d')
    root_folder = "E:\BlaBlaCar\python_without_airflow\python_results" #change the path as per you system.
    return os.path.join(root_folder, year, month, day)
