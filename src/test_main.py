import json
import os
import shutil
import unittest
from unittest.mock import patch

import pandas as pd

from main import (
    append_to_table,
    check_directory,
    convert_to_tabular,
    create_customer_table,
    create_layers,
    create_product_table,
    create_transaction_table,
    get_basic_statistics,
    get_last_transaction,
    get_total_spent,
    load_bronze_data,
    load_silver_data,
    open_file,
    rearrange_data,
    save_bronze_data,
    save_golden_data,
    save_silver_data,
)


class TestMain(unittest.TestCase):
    def remove_data_directories(self):
        # Clean up the created directories
        data_directory = "data"
        directories = ["bronze", "silver", "gold"]
        for directory in directories:
            directory_path = os.path.join(data_directory, directory)
            if not check_directory(directory_path):
                shutil.rmtree(directory_path)

    def setUp(self):
        self.remove_data_directories()

    def test_open_file(self):
        # Test when file exists
        file_path = '/path/to/existing/file.json'
        expected_data = {'key': 'value'}
        with open(file_path, 'w') as f:
            json.dump(expected_data, f)
        data = open_file(file_path)
        self.assertEqual(data, expected_data)

        # Test when file does not exist
        file_path = '/path/to/nonexistent/file.json'
        with self.assertRaises(FileNotFoundError):
            open_file(file_path)

    def test_check_directory(self):
        # Test when directory does not exist
        directory_path = '/path/to/nonexistent/directory'
        self.assertTrue(check_directory(directory_path))

        # Test when directory exists
        directory_path = '/path/to/existing/directory'
        os.makedirs(directory_path, exist_ok=True)
        self.assertFalse(check_directory(directory_path))

        # Clean up the created directory
        os.rmdir(directory_path)

    def test_skip_create_of_directories_if_exists(self):
        # Test the creation of directories
        data_directory = "data"
        directories = ["bronze", "silver", "gold"]

        # Mock the check_directory function to always return True
        with patch('main.check_directory', return_value=False):
            create_layers()

        # Check if the directories are were not created
        for directory in directories:
            directory_path = os.path.join(data_directory, directory)
            self.assertFalse(os.path.exists(directory_path))

        # Clean up the created directories

    @staticmethod
    def test_convert_to_tabular():
        data = {'key': 'value'}
        expected_df = pd.DataFrame({'key': ['value']})
        df = convert_to_tabular(data)
        pd.testing.assert_frame_equal(df, expected_df)
        data = [{'key': 'value1'}, {'key': 'value2'}]
        expected_df = pd.DataFrame({'key': ['value1', 'value2']})
        df = convert_to_tabular(data)
        pd.testing.assert_frame_equal(df, expected_df)
        data = []
        expected_df = pd.DataFrame()
        df = convert_to_tabular(data)
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_basic_statistics(self):
        # Test when the DataFrame is not empty
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        expected_result = df.describe()
        result = get_basic_statistics(df)
        pd.testing.assert_frame_equal(result, expected_result)

        # Test when the DataFrame is empty
        df = pd.DataFrame()
        with self.assertRaises(ValueError):
            result = get_basic_statistics(df)

    def test_save_bronze_data(self):
        # Create a temporary DataFrame for testing
        df_bronze_data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

        # Mock the Timestamp function to return a fixed date
        with patch('main.pd.Timestamp') as mock_timestamp:
            mock_timestamp.return_value.strftime.return_value = '2022-01-01'

            # Call the function
            save_bronze_data(df_bronze_data)

        # Check if the directory and file were created
        expected_bronze_path = 'data/bronze'
        expected_directory_path = 'data/bronze/2022-01-01'
        expected_file_path = 'data/bronze/2022-01-01/data.csv'
        self.assertTrue(os.path.exists(expected_directory_path))
        self.assertTrue(os.path.exists(expected_file_path))

        # Check if the file content matches the DataFrame
        df_loaded = pd.read_csv(expected_file_path)
        pd.testing.assert_frame_equal(df_loaded, df_bronze_data)

        # Clean up the created directory and file
        os.remove(expected_file_path)
        os.rmdir(expected_directory_path)
        os.rmdir(expected_bronze_path)

    def test_load_bronze_data(self):
        # Create a temporary DataFrame for testing
        df_bronze_data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

        # Create the directory and file for the test data
        today = pd.Timestamp("today").strftime("%Y-%m-%d")
        directory_path = f"data/bronze/{today}"
        file_path = f"{directory_path}/data.csv"
        os.makedirs(directory_path, exist_ok=True)
        df_bronze_data.to_csv(file_path, index=False)

        # Call the function
        result = load_bronze_data()

        # Check if the loaded DataFrame matches the test data
        pd.testing.assert_frame_equal(result, df_bronze_data)

        # Clean up the created directory and file
        os.remove(file_path)
        os.rmdir(directory_path)

    def test_rearrange_data(self):
        # Create a sample DataFrame for testing
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["John", "Jane"],
            "email": ["john@example.com", "jane@example.com"],
            "signup_date": ["2022-01-01", "2022-01-02"],
            "last_purchase": ["2022-02-01", "2022-02-02"],
            "total_spent": [100, 200],
            "transactions": ["[{'id': 1, 'amount': 50}, {'id': 2, 'amount': 75}]",
                             "[{'id': 3, 'amount': 100}, {'id': 4, 'amount': 150}]"]
        })

        # Define the expected result DataFrame
        expected_result = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "amount": [50, 75, 100, 150],
            "customer_id": [1, 1, 2, 2],
            "customer_name": ["John", "John", "Jane", "Jane"],
            "customer_email": ["john@example.com", "john@example.com", "jane@example.com", "jane@example.com"],
            "signup_date": ["2022-01-01", "2022-01-01", "2022-01-02", "2022-01-02"],
            "last_purchase": ["2022-02-01", "2022-02-01", "2022-02-02", "2022-02-02"],
            "total_spent": [100, 100, 200, 200],
        })

        # Call the function
        result = rearrange_data(df)

        # Check if the result matches the expected result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_create_transaction_table(self):
        # Create a sample DataFrame for testing
        df = pd.DataFrame({
            "transaction_id": [1, 2, 3],
            "date": ["2022-01-01", "2022-01-02", "2022-01-03"],
            "amount": [100, 200, 300],
            "product_id": [101, 102, 103],
            "customer_id": [1, 2, 3]
        })

        # Define the expected result DataFrame
        expected_result = pd.DataFrame({
            "transaction_id": [1, 2, 3],
            "date": ["2022-01-01", "2022-01-02", "2022-01-03"],
            "amount": [100, 200, 300],
            "product_id": [101, 102, 103],
            "customer_id": [1, 2, 3]
        })

        # Call the function
        result = create_transaction_table(df)

        # Check if the result matches the expected result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_create_customer_table(self):
        # Create a sample DataFrame for testing
        df = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "customer_name": ["John", "Jane", "Alice"],
            "customer_email": ["john@example.com", "jane@example.com", "alice@example.com"],
            "signup_date": ["2022-01-01", "2022-01-02", "2022-01-03"]
        })

        # Define the expected result DataFrame
        expected_result = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "customer_name": ["John", "Jane", "Alice"],
            "customer_email": ["john@example.com", "jane@example.com", "alice@example.com"],
            "signup_date": ["2022-01-01", "2022-01-02", "2022-01-03"]
        })

        # Call the function
        result = create_customer_table(df)

        # Check if the result matches the expected result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_create_product_table(self):
        # Create a sample DataFrame for testing
        df = pd.DataFrame({
            "product_id": [101, 102, 103, 101, 102],
            "product_name": ["Product A", "Product B", "Product C", "Product A", "Product B"]
        })

        # Define the expected result DataFrame
        expected_result = pd.DataFrame({
            "product_id": [101, 102, 103],
            "product_name": ["Product A", "Product B", "Product C"]
        })

        # Call the function
        result = create_product_table(df)

        # Check if the result matches the expected result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_save_silver_data(self):
        # Create a sample DataFrame for testing
        df_silver_data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        table_name = 'my_table'

        # Mock the Timestamp function to return a fixed date
        with patch('main.pd.Timestamp') as mock_timestamp:
            mock_timestamp.return_value.strftime.return_value = '2022-01-01'

            # Call the function
            save_silver_data(df_silver_data, table_name)

        # Check if the directory and file were created
        expected_silver_path = 'data/silver'
        expected_directory_path = 'data/silver/2022-01-01'
        expected_file_path = 'data/silver/2022-01-01/my_table.csv'
        self.assertTrue(os.path.exists(expected_directory_path))
        self.assertTrue(os.path.exists(expected_file_path))

        # Check if the file content matches the DataFrame
        df_loaded = pd.read_csv(expected_file_path)
        pd.testing.assert_frame_equal(df_loaded, df_silver_data)

        # Clean up the created directory and file
        os.remove(expected_file_path)
        os.rmdir(expected_directory_path)
        os.rmdir(expected_silver_path)

    def test_get_last_transaction(self):
        # Test when the input is a valid JSON string
        input_data = "[{'date': '2022-01-01'}, {'date': '2022-01-02'}, {'date': '2022-01-03'}]"
        expected_result = pd.to_datetime('2022-01-03')
        result = get_last_transaction(input_data)
        self.assertEqual(result, expected_result)

        # Test when the input is an empty JSON string
        input_data = "[]"
        expected_result = None
        result = get_last_transaction(input_data)
        self.assertEqual(result, expected_result)

        # Test when the input is an invalid JSON string
        input_data = "{'date': '2022-01-01'}"
        expected_result = 0
        result = get_last_transaction(input_data)
        self.assertEqual(result, expected_result)

    def test_get_total_spent(self):
        # Test when the input is a valid JSON string
        input_json = "[{'id': 1, 'amount': 50}, {'id': 2, 'amount': 75}]"
        expected_total = 125
        result = get_total_spent(input_json)
        self.assertEqual(result, expected_total)

        # Test when the input is an empty JSON string
        input_json = "[]"
        expected_total = 0
        result = get_total_spent(input_json)
        self.assertEqual(result, expected_total)

        # Test when the input is an invalid JSON string
        input_json = "{'id': 1, 'amount': 50}"
        expected_total = 0
        result = get_total_spent(input_json)
        self.assertEqual(result, expected_total)

        # Test when the input is not a string
        input_json = 123
        expected_total = 0
        result = get_total_spent(input_json)
        self.assertEqual(result, expected_total)

    def test_load_silver_data(self):
        # Create a temporary DataFrame for testing
        df_silver_data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        table_name = 'my_table'

        # Create the directory and file for the test data
        today = pd.Timestamp("today").strftime("%Y-%m-%d")
        directory_path = f"data/silver/{today}"
        file_path = f"{directory_path}/{table_name}.csv"
        os.makedirs(directory_path, exist_ok=True)
        df_silver_data.to_csv(file_path, index=False)

        # Call the function
        result = load_silver_data(table_name)

        # Check if the loaded DataFrame matches the test data
        pd.testing.assert_frame_equal(result, df_silver_data)

        # Clean up the created directory and file
        os.remove(file_path)
        os.rmdir(directory_path)

    def test_append_to_table(self):
        # Create a sample DataFrame for testing
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

        # Create a temporary file for testing
        file_path = 'test_data.csv'
        df.to_csv(file_path, index=False)

        # Call the function
        append_to_table(df, file_path)

        # Read the updated file
        updated_data = pd.read_csv(file_path)

        # Define the expected result DataFrame
        expected_result = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

        # Check if the updated file matches the expected result
        pd.testing.assert_frame_equal(updated_data, expected_result)

        # Clean up the temporary file
        os.remove(file_path)

    def test_save_golden_data(self):
        # Create a sample DataFrame for testing
        df_golden_data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        table_name = 'my_table'

        # Create gold directory
        os.makedirs('data/gold', exist_ok=True)

        # Define the expected file path
        expected_file_path = 'data/gold/my_table.csv'

        # Call the function for the first time
        save_golden_data(df_golden_data, table_name)

        # Check if the file was created and the content matches the DataFrame
        self.assertTrue(os.path.exists(expected_file_path))
        df_loaded = pd.read_csv(expected_file_path)
        pd.testing.assert_frame_equal(df_loaded, df_golden_data)

        # Call the function again with a different DataFrame
        df_golden_data_updated = pd.DataFrame({'A': [4, 5, 6], 'B': [7, 8, 9]})
        save_golden_data(df_golden_data_updated, table_name)

        # Check if the file content was appended with the updated DataFrame
        df_loaded_updated = pd.read_csv(expected_file_path).reset_index(drop=True)
        df_expected_updated = pd.concat(
            [df_golden_data, df_golden_data_updated]).reset_index(drop=True)
        pd.testing.assert_frame_equal(df_loaded_updated, df_expected_updated)

        # Clean up the created file
        os.remove(expected_file_path)


if __name__ == '__main__':
    unittest.main()
