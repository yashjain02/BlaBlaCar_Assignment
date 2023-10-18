from commons import fetch_data, data_transformation, date_partitioning


if __name__ == "__main__":
    # Define the base URL for the API and endpoint
    base_url = "http://v0.ovapi.nl/"
    endpoint = "line/"
    url = f"{base_url}{endpoint}"
    main_key = "Line"
    filename = f"{main_key}_data.csv"
    data = fetch_data(url)
    folder_path = date_partitioning()
    data_transformation(data, folder_path, filename, main_key)
