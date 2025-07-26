"""
Comprehensive tests for the SqliteInterface class.
"""

import unittest
import tempfile
import os
import pandas as pd
import sqlite3
from unittest.mock import patch, MagicMock
import sys

# Add the src directory to the Python path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_interface import SqliteInterface


class TestSqliteInterface(unittest.TestCase):
    """Test class for SqliteInterface functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary database file for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.interface = SqliteInterface(self.db_path)
        
        # Create sample data for testing
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
        })
        
        self.additional_data = pd.DataFrame({
            'id': [4, 5],
            'name': ['David', 'Eve'],
            'age': [28, 32],
            'email': ['david@example.com', 'eve@example.com']
        })

    def tearDown(self):
        """Clean up after each test method."""
        # Close the database connection
        if hasattr(self.interface, '_connection'):
            self.interface._connection.close()
        
        # Remove the temporary database file
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_init_with_existing_database(self):
        """Test initialization with an existing database file."""
        # The database should be created and connection established
        self.assertTrue(os.path.exists(self.db_path))
        self.assertIsInstance(self.interface._connection, sqlite3.Connection)
        self.assertEqual(self.interface._db_path, self.db_path)

    def test_init_with_non_existing_database_create_true(self):
        """Test initialization with non-existing database file and create_if_not_exists=True."""
        # Remove the existing temp file
        os.unlink(self.db_path)
        
        # Create interface with non-existing file
        interface = SqliteInterface(self.db_path, create_if_not_exists=True)
        
        self.assertTrue(os.path.exists(self.db_path))
        self.assertIsInstance(interface._connection, sqlite3.Connection)
        interface._connection.close()

    def test_init_with_non_existing_database_create_false(self):
        """Test initialization with non-existing database file and create_if_not_exists=False."""
        # Remove the existing temp file
        os.unlink(self.db_path)
        
        with self.assertRaises(FileNotFoundError):
            SqliteInterface(self.db_path, create_if_not_exists=False)

    def test_init_creates_directory_if_not_exists(self):
        """Test that initialization creates the directory structure if it doesn't exist."""
        # Create a path with non-existing directories
        nested_path = os.path.join(tempfile.gettempdir(), 'test_dir', 'nested', 'test.db')
        
        try:
            interface = SqliteInterface(nested_path, create_if_not_exists=True)
            self.assertTrue(os.path.exists(nested_path))
            interface._connection.close()
        finally:
            # Clean up
            if os.path.exists(nested_path):
                os.unlink(nested_path)
            # Remove the created directories
            import shutil
            test_dir = os.path.join(tempfile.gettempdir(), 'test_dir')
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)

    def test_write_table_new_table(self):
        """Test writing data to a new table."""
        table_name = 'test_table'
        
        # Write the sample data to the table
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Verify the table was created and data was written
        result = pd.read_sql_query(f"SELECT * FROM {table_name}", self.interface._connection)
        pd.testing.assert_frame_equal(result, self.sample_data)

    def test_write_table_replace(self):
        """Test writing data to an existing table with replace option."""
        table_name = 'test_table'
        
        # Write initial data
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Write new data with replace option
        new_data = pd.DataFrame({'id': [10], 'name': ['New User'], 'age': [40], 'email': ['new@example.com']})
        self.interface.write_table(table_name, new_data, if_exists='replace')
        
        # Verify the table was replaced
        result = pd.read_sql_query(f"SELECT * FROM {table_name}", self.interface._connection)
        pd.testing.assert_frame_equal(result, new_data)

    def test_write_table_append(self):
        """Test writing data to an existing table with append option."""
        table_name = 'test_table'
        
        # Write initial data
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Append additional data
        self.interface.write_table(table_name, self.additional_data, if_exists='append')
        
        # Verify the data was appended
        result = pd.read_sql_query(f"SELECT * FROM {table_name}", self.interface._connection)
        expected = pd.concat([self.sample_data, self.additional_data], ignore_index=True)
        pd.testing.assert_frame_equal(result, expected)

    def test_read_table_existing(self):
        """Test reading data from an existing table."""
        table_name = 'test_table'
        
        # Write data first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Read the data back
        result = self.interface.read_table(table_name)
        
        pd.testing.assert_frame_equal(result, self.sample_data)

    def test_read_table_non_existing(self):
        """Test reading data from a non-existing table."""
        with self.assertRaises(ValueError) as context:
            self.interface.read_table('non_existing_table')
        
        self.assertIn("does not exist", str(context.exception))

    def test_add_row_valid_data(self):
        """Test adding a row with valid data to an existing table."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Add a new row
        new_row = {
            'id': '6',
            'name': "'Frank'",
            'age': '29',
            'email': "'frank@example.com'"
        }
        self.interface.add_row(table_name, new_row)
        
        # Verify the row was added
        result = pd.read_sql_query(f"SELECT * FROM {table_name} WHERE id = 6", self.interface._connection)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['name'], 'Frank')

    def test_add_row_non_existing_table(self):
        """Test adding a row to a non-existing table."""
        new_row = {'id': '1', 'name': "'Test'"}
        
        with self.assertRaises(ValueError) as context:
            self.interface.add_row('non_existing_table', new_row)
        
        self.assertIn("does not exist", str(context.exception))

    def test_add_column_to_existing_table(self):
        """Test adding a column to an existing table."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Add a new column
        self.interface.add_column(table_name, 'salary', 'INTEGER')
        
        # Verify the column was added
        cursor = self.interface._connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        self.assertIn('salary', columns)

    def test_add_column_non_existing_table(self):
        """Test adding a column to a non-existing table."""
        with self.assertRaises(ValueError) as context:
            self.interface.add_column('non_existing_table', 'new_column', 'TEXT')
        
        self.assertIn("does not exist", str(context.exception))

    def test_check_table_exists_true(self):
        """Test checking if an existing table exists."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Check if table exists
        self.assertTrue(self.interface._check_table_exists(table_name))

    def test_check_table_exists_false(self):
        """Test checking if a non-existing table exists."""
        self.assertFalse(self.interface._check_table_exists('non_existing_table'))

    def test_check_columns_all_exist(self):
        """Test checking columns when all specified columns exist."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Check existing columns
        result = self.interface._check_columns(table_name, ['id', 'name', 'age'])
        self.assertEqual(result, [True, True, True])

    def test_check_columns_some_exist(self):
        """Test checking columns when some specified columns exist."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Check mix of existing and non-existing columns
        result = self.interface._check_columns(table_name, ['id', 'non_existing', 'name'])
        self.assertEqual(result, [True, False, True])

    def test_check_columns_none_exist(self):
        """Test checking columns when none of the specified columns exist."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Check non-existing columns
        result = self.interface._check_columns(table_name, ['col1', 'col2'])
        self.assertEqual(result, [False, False])

    def test_check_columns_non_existing_table(self):
        """Test checking columns on a non-existing table."""
        with self.assertRaises(ValueError) as context:
            self.interface._check_columns('non_existing_table', ['col1'])
        
        self.assertIn("does not exist", str(context.exception))

    def test_check_and_add_columns_add_missing(self):
        """Test checking and adding columns when some are missing."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Check and add columns (some existing, some new)
        columns = ['id', 'salary', 'department']
        column_types = ['INTEGER', 'INTEGER', 'TEXT']
        
        self.interface.check_and_add_columns(table_name, columns, column_types)
        
        # Verify all columns now exist
        cursor = self.interface._connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        for col in columns:
            self.assertIn(col, existing_columns)

    def test_check_and_add_columns_all_exist(self):
        """Test checking and adding columns when all already exist."""
        table_name = 'test_table'
        
        # Create table first
        self.interface.write_table(table_name, self.sample_data, if_exists='fail')
        
        # Check existing columns
        columns = ['id', 'name', 'age']
        column_types = ['INTEGER', 'TEXT', 'INTEGER']
        
        # This should not raise any errors
        self.interface.check_and_add_columns(table_name, columns, column_types)
        
        # Verify columns still exist
        result = self.interface._check_columns(table_name, columns)
        self.assertEqual(result, [True, True, True])

    def test_check_and_add_columns_non_existing_table(self):
        """Test checking and adding columns to a non-existing table."""
        with self.assertRaises(ValueError) as context:
            self.interface.check_and_add_columns('non_existing_table', ['col1'], ['TEXT'])
        
        self.assertIn("does not exist", str(context.exception))

    def test_upsert_existing_table(self):
        """Test upsert operation on an existing table."""
        table_name = 'test_table'
        
        # Create table with initial data and PRIMARY KEY constraint on id
        # Using raw SQL to ensure proper PRIMARY KEY constraint
        cursor = self.interface._connection.cursor()
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT,
                age INTEGER
            )
        """)
        
        # Insert initial data
        initial_data = [
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 35)
        ]
        cursor.executemany(f"INSERT INTO {table_name} (id, name, age) VALUES (?, ?, ?)", initial_data)
        self.interface._connection.commit()
        
        # Create upsert data (update existing + add new)
        upsert_data = pd.DataFrame({
            'id': [2, 4],  # Update id=2, insert id=4
            'name': ['Bob Updated', 'David'],
            'age': [31, 28]
        })
        
        self.interface.upsert(table_name, upsert_data)
        
        # Verify the results
        result = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id", self.interface._connection)
        
        # Should have 4 rows: original 1,3 + updated 2 + new 4
        self.assertEqual(len(result), 4)
        
        # Check that id=2 was updated
        bob_row = result[result['id'] == 2].iloc[0]
        self.assertEqual(bob_row['name'], 'Bob Updated')
        self.assertEqual(bob_row['age'], 31)
        
        # Check that id=4 was inserted
        david_row = result[result['id'] == 4].iloc[0]
        self.assertEqual(david_row['name'], 'David')

    def test_upsert_non_existing_table(self):
        """Test upsert operation on a non-existing table."""
        upsert_data = pd.DataFrame({
            'id': [1],
            'name': ['Test'],
            'age': [25]
        })
        
        with self.assertRaises(ValueError) as context:
            self.interface.upsert('non_existing_table', upsert_data)
        
        self.assertIn("does not exist", str(context.exception))

    def test_upsert_with_different_column_count(self):
        """Test upsert with data that has different number of columns than initial table."""
        table_name = 'test_table'
        
        # Create table with PRIMARY KEY constraint
        cursor = self.interface._connection.cursor()
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT,
                age INTEGER
            )
        """)
        
        # Insert initial data
        cursor.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
        self.interface._connection.commit()
        
        # Add a new column to the table
        self.interface.add_column(table_name, 'email', 'TEXT')
        
        # Try to upsert with 4 columns (now this should work)
        upsert_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice Updated', 'Bob'],
            'age': [26, 30],
            'email': ['alice@example.com', 'bob@example.com']
        })
        
        # This should now work with the dynamic placeholder implementation
        self.interface.upsert(table_name, upsert_data)
        
        # Verify the results
        result = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id", self.interface._connection)
        self.assertEqual(len(result), 2)
        
        # Check that Alice was updated
        alice_row = result[result['id'] == 1].iloc[0]
        self.assertEqual(alice_row['name'], 'Alice Updated')
        self.assertEqual(alice_row['email'], 'alice@example.com')
        
        # Check that Bob was inserted
        bob_row = result[result['id'] == 2].iloc[0]
        self.assertEqual(bob_row['name'], 'Bob')

    def test_upsert_without_primary_key_constraint(self):
        """Test upsert operation on a table without PRIMARY KEY constraint."""
        table_name = 'test_table'
        
        # Create table without PRIMARY KEY constraint (using write_table)
        initial_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30]
        })
        self.interface.write_table(table_name, initial_data, if_exists='fail')
        
        # Try to upsert (this should fail due to lack of PRIMARY KEY constraint)
        upsert_data = pd.DataFrame({
            'id': [2, 3],
            'name': ['Bob Updated', 'Charlie'],
            'age': [31, 35]
        })
        
        with self.assertRaises(sqlite3.OperationalError) as context:
            self.interface.upsert(table_name, upsert_data)
        
        self.assertIn("PRIMARY KEY or UNIQUE constraint", str(context.exception))

    def test_complex_scenario_full_workflow(self):
        """Test a complex scenario combining multiple operations."""
        table_name = 'users'
        
        # 1. Create initial table
        initial_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30]
        })
        self.interface.write_table(table_name, initial_data, if_exists='fail')
        
        # 2. Add new columns
        self.interface.check_and_add_columns(
            table_name, 
            ['email', 'department'], 
            ['TEXT', 'TEXT']
        )
        
        # 3. Add individual rows
        self.interface.add_row(table_name, {
            'id': '3',
            'name': "'Charlie'",
            'age': '35',
            'email': "'charlie@example.com'",
            'department': "'Engineering'"
        })
        
        # 4. Read and verify
        result = self.interface.read_table(table_name)
        self.assertEqual(len(result), 3)
        
        # Check that new columns exist
        self.assertIn('email', result.columns)
        self.assertIn('department', result.columns)
        
        # Check that Charlie was added correctly
        charlie_row = result[result['id'] == 3].iloc[0]
        self.assertEqual(charlie_row['name'], 'Charlie')
        self.assertEqual(charlie_row['email'], 'charlie@example.com')

    def test_edge_case_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        table_name = 'empty_table'
        empty_df = pd.DataFrame(columns=['id', 'name'])
        
        # Writing empty DataFrame should work
        self.interface.write_table(table_name, empty_df, if_exists='fail')
        
        # Reading should return empty DataFrame with correct columns
        result = self.interface.read_table(table_name)
        self.assertEqual(len(result), 0)
        self.assertListEqual(list(result.columns), ['id', 'name'])

    def test_edge_case_special_characters_in_data(self):
        """Test handling of special characters in data."""
        table_name = 'special_chars'
        
        special_data = pd.DataFrame({
            'id': [1, 2],
            'name': ["O'Connor", 'Smith "The Great"'],
            'description': ['Line 1\nLine 2', 'Text with; semicolon']
        })
        
        self.interface.write_table(table_name, special_data, if_exists='fail')
        result = self.interface.read_table(table_name)
        
        pd.testing.assert_frame_equal(result, special_data)


if __name__ == '__main__':
    unittest.main()
