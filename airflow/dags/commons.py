import requests
import csv
import os
import ast


def fetch_data(url: str, datalake_path :str, rawdata_filename:str, **context)->None:
    """
    This function fetchs the data from the given API
    Args:
        url (str): The URL of the API from which data will be fetched.
        datalake_path: Path of datalake where raw file is stored.
        rawdata_filename: Name of the raw data file.
        **context: This parameter gets the task details, like execution date, start date etc.

    Returns:
        Writes raw data in csv format or None in case of errors.
    """
    try:
        response = requests.get(url)
        data = response.json()
        date_partition=date_partitioning(**context)
        os.makedirs(f"{datalake_path}/{date_partition}", exist_ok=True) #checks if the dir exists or creats one. 
        with open( f"{datalake_path}/{date_partition}/{rawdata_filename}", 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data.keys())
            writer.writeheader()
            writer.writerow(data)
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as e:
        print(f"An error occurred: {e}")


def read_from_csv(datalake_path : str, date_partition : str, rawdata_file : str) -> dict:
    """
    This Function read data from csv file and return the data.
    Args:
        rawdata_filename: Name of the raw data file.
        datalake_path: Path of datalake where raw file is stored.
        date_partition: File path with date patitioned folder.
    Returns:
        Return the data of csv file in dictionary.
    """
    data_dict = {}  # Create an empty dictionary to store the data
    with open(f'/{datalake_path}/{date_partition}/{rawdata_file}', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            for header, value in row.items():
                # Use ast.literal_eval to convert the string to a dictionary
                data_dict[header] = ast.literal_eval(value)
    return data_dict


def data_transformation(datalake_path : str, data_warehouse_path : str, final_file : str,
                        rawdata_file : str, **context) -> None:
    """
    This Function does data tranformation for csv data. Its Unnest the inner dictionary 
    and writes main key as an separate column corresponding to its inner value and writes 
    to csv.
    Args:
        datalake_path: Path of datalake where raw file is stored.
        data_warehouse_path: path to the datawarehouse folder.
        final_file: Name of the final file stored in data warehouse.
        **context: This parameter gets the task details, like execution date, start date etc.
        rawdata_filename: Name of the raw data file.

    Returns:
        Writes the transformed data to a CSV file in the data warehouse folder in date partition.
    """
    main_key="Line"
    date_partition = date_partitioning(**context)
    data = read_from_csv(datalake_path, date_partition,rawdata_file)
    if data:
        # Create the folder path if it doesn't exist
        os.makedirs(f'/{data_warehouse_path}/{date_partition}', exist_ok=True)
        # Combine the folder path and filename to get the full file path
        file_path = os.path.join(f"/{data_warehouse_path}/{date_partition}", f"{final_file}")
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


def date_partitioning(**context) -> str:
    """
    This function creates a dicrectory based on the date partitioning
    Args:
        context: contains the details of task.
    """
    execution_date = context['logical_date']
    year = str(execution_date.year)
    month = execution_date.strftime('%m')
    day = execution_date.strftime('%d')
    return os.path.join(year, month, day)
