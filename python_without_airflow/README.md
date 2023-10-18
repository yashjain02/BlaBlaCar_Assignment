# BlaBlaCar

This README provides an overview and usage guide for a set of Python functions that facilitate data retrieval, transformation, and storage. These functions are intended to assist in fetching data from an API, transforming it into a structured format, and storing it as CSV files.

## About 
This Project intents to query data from Public API for "Transport for The Netherlands" which provide information about OVAPI, country-wide public transport and store it in a CSV file.<br />
API Description: https://github.com/koch-t/KV78Turbo-OVAPI/wiki.<br />
We will use Per Line Endpoint<br />
Base_url : http://v0.ovapi.nl/<br />
Endpoint: /line/<br />
Authorization: Not needed <br />

## Files 
    python_without_airflow/
    |-scripts/
    |     |_ commons.py
    |     |_ fetch_line_data.py 
    |     |_ __int__.py
    |
    |-Tests/
    |      |_fetch_data_test.py
    |-Readme.md

## About the Files
- **fetch_line_data.py**:<br />
    This file is the main file where the script runs. The code is broken down in functions which are in commons.py file.

- **commons.py**:<br />
    This file contains only functions which are used by fetch_line_data.py. Some are generic function and some are very specfic to this project. The function definetion are below.<br />

    **Functions**:<br />
    - **fetch_data(url: str) -> json**<br />
        This function fetches data from a given API using the provided URL.<br />

        Parameters:<br />
        url (str): The URL of the API from which data will be fetched.<br />

        Returns:<br />
        json: The data retrieved from the API, or None in case of errors.<br />
    
    - **data_transformation(data: dict, folder_path: str, filename: str, main_key: str) -> None**<br />
        This function performs data transformation on the fetched data and writes it to a CSV file. It un-nests inner dictionaries, adds a main key as a separate column, and writes the data to a CSV file.<br />

        Parameters:<br />
        data (dict): Data fetched from the API.<br />
        folder_path (str): The folder where the CSV file should be stored.<br />
        filename (str): The name of the CSV file.<br />
        main_key (str): The key of the dictionary that is converted into a separate column and appended to each sub-dictionary.<br />

        Output:<br />
        Writes the transformed data to a CSV file in the specified folder.<br />

    - **write_to_csv(file_path: str, fieldnames: list, csv_data: list) -> None**<br />
        This function writes data to a CSV file.<br />

        Parameters:<br />
        file_path (str): The path to the CSV file.<br />
        fieldnames (list): Names of the header columns.<br />
        csv_data (list): Data to be written to the CSV file.<br />

        Output:<br />
        Writes data to the specified CSV file.<br />
    
    - **date_partitioning() -> str**<br />
        This function creates a directory structure based on the current date. It's intended for date-based partitioning of data storage.<br />

        Returns:<br />
        str: The path to the directory structure based on the current date.
        Usage

## Running the Script

To run the Python script that contains the data processing functions, follow these steps:

1. **Clone the Repository:**
   If you haven't already, clone this repository to your local machine using Git.

   ```bash
   git clone https://github.com/yashjain02/BlaBlaCar.git
   cd BlaBlaCar
   
2. **change the parameter**<br />
    In scripts/commons.py, change the path in data_partitioning() where you want to store the final file.

3. **Running the Script**

    ```Python
    Python scripts\fetch_line_data.py
    ```

## Running the test cases

Unit test cases have been written for all the functions. The test scripts are in test folder in fetch_data_test.py file.
To run the tests run the below command.
```Python
python -m unittest tests/fetch_data_test.py
```

## Data Flow in the script

Below is the step by step process how the data is travelled.
1. By running fetch_line_data.py file it call fetch_data function from commons.py with API url as the parameter.
2. fetch_data function queries the API and returns the data in json format if not returns None or return message with an error.
3. Then I used concept of date partitioning in datawarehouse, reasone for that is readability and backfilling is faster in airflow. date_partitioning() will create the folder path as per current date.
4. Both the data and folder path is sent to data_tranformation() which tranform the data and write to csv file in structed format. 
5. **Why data tranformation?**<br />
    When you run the airflow script you will find in result/datalake/raw_data.csv that the data is unstructed. The Line are as headers and the value are dictionary. For data analyst it is difficult to create a dashboard. 
6. data_transformation() creates a new column name 'Line' where the value is the Line name. And the Sub dictionary are created as column with respective Line name. 
7. The final result can be found in python_result folder. It is in date partition.

