import csv
import unittest
from unittest.mock import patch, Mock
from scripts.commons import (
    data_transformation,
    write_to_csv,
    date_partitioning,
    fetch_data
)
import os
import tempfile
from datetime import datetime


class TestFunction(unittest.TestCase):

    @patch('datetime.datetime')
    def test_date_partitioning(self, mock_datetime):
        # Use a fixed date for testing
        fixed_date = datetime(2023, 10, 15)
        mock_datetime.now.return_value = fixed_date
        result = date_partitioning()
        # Expected path based on the fixed date
        expected_path = os.path.join('E:\BlaBlaCar\python_results', '2023', '10', '16')
        self.assertEqual(result, expected_path)


    def test_write_to_csv(self):
        # Test data
        csv_data = [
            {"name": "Alice", "age": '30'},
            {"name": "Bob", "age": '25'},
            {"name": "Charlie", "age": '35'},
        ]
        fieldnames = ["name", "age"]
        csvfile = "test.csv"
        # Call the function to write data to the CSV file
        write_to_csv(csvfile, fieldnames, csv_data)
        # Check if the file has been created and contains the expected data
        self.assertTrue(os.path.isfile(csvfile))
        with open(csvfile, "r", newline="") as file:
            reader = csv.DictReader(file)
            written_data = list(reader)
        self.assertEqual(written_data, csv_data)
        # Clean up: Remove the test CSV file
        os.remove(csvfile)


    def setUp(self):
        # Create a temporary directory to store CSV files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.folder_path = self.temp_dir.name


    def tearDown(self):
        # Clean up the temporary directory
        self.temp_dir.cleanup()


    def test_data_transformation(self):
        # Sample data for testing
        data = {
            'A': {'x': 1, 'y': 2}
        }
        filename = 'test_data.csv'
        main_key = 'main_key'

        # Call the data_transformation function
        data_transformation(data, self.folder_path, filename, main_key)
        # Check if the CSV file was created
        csv_file_path = os.path.join(self.folder_path, filename)
        self.assertTrue(os.path.isfile(csv_file_path))
        # Read and verify the content of the CSV file
        with open(csv_file_path, 'r') as csv_file:
            lines = csv_file.readlines()
            # Check the header line
            self.assertEqual(lines[0].strip(), f'{main_key},{",".join(sorted(data["A"].keys()))}')
            # Check the data lines
            data_lines = [line.strip() for line in lines[1:]]
            expected_lines = ['A,1,2']
            for expected, actual in zip(expected_lines, data_lines):
                self.assertEqual(expected, actual)

    @patch('requests.get')
    def test_fetch_data_success(self, mock_get):
        # Mock a successful API response
        mock_response = Mock()
        mock_response.json.return_value = {'data': 'example data'}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        url = 'https://example.com/api'
        result = fetch_data(url)

        self.assertEqual(result, {'data': 'example data'})

    @patch('requests.get')
    def test_fetch_data_http_error(self, mock_get):
        # Mock an HTTP error response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception('HTTP error')
        mock_get.return_value = mock_response

        url = 'https://example.com/api'
        result = fetch_data(url)

        self.assertIsNone(result)

    @patch('requests.get')
    def test_fetch_data_request_error(self, mock_get):
        # Mock a request error
        mock_get.side_effect = Exception('Request error')

        url = 'https://example.com/api'
        result = fetch_data(url)

        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
