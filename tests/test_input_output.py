"""
Comprehensive tests for the input_output module.
"""

import unittest
import tempfile
import os
import json
import sys
from unittest.mock import patch, mock_open

# Add the src directory to the Python path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from input_output import read_json


class TestReadJson(unittest.TestCase):
    """Test class for read_json function."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Sample JSON data for testing
        self.sample_data = {
            "name": "John Doe",
            "age": 30,
            "city": "New York",
            "hobbies": ["reading", "swimming"],
            "active": True
        }
        
        # Create a valid JSON file for testing
        self.valid_json_file = os.path.join(self.temp_dir, "valid_test.json")
        with open(self.valid_json_file, 'w') as f:
            json.dump(self.sample_data, f)
        
        # Create an invalid JSON file for testing
        self.invalid_json_file = os.path.join(self.temp_dir, "invalid_test.json")
        with open(self.invalid_json_file, 'w') as f:
            f.write('{"invalid": json content}')
        
        # Create a non-JSON file for testing
        self.non_json_file = os.path.join(self.temp_dir, "test.txt")
        with open(self.non_json_file, 'w') as f:
            f.write("This is a text file, not JSON")

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove temporary files and directory
        for file_path in [self.valid_json_file, self.invalid_json_file, self.non_json_file]:
            if os.path.exists(file_path):
                os.remove(file_path)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    def test_read_valid_json_file(self):
        """Test reading a valid JSON file."""
        result = read_json(self.valid_json_file)
        
        # Assert that the returned data matches the expected data
        self.assertEqual(result, self.sample_data)
        self.assertIsInstance(result, dict)

    def test_read_json_with_nested_data(self):
        """Test reading a JSON file with nested data structures."""
        nested_data = {
            "user": {
                "personal": {
                    "name": "Jane Doe",
                    "age": 25
                },
                "preferences": {
                    "theme": "dark",
                    "notifications": True
                }
            },
            "metadata": {
                "created": "2025-01-01",
                "version": "1.0"
            }
        }
        
        nested_json_file = os.path.join(self.temp_dir, "nested_test.json")
        with open(nested_json_file, 'w') as f:
            json.dump(nested_data, f)
        
        try:
            result = read_json(nested_json_file)
            self.assertEqual(result, nested_data)
            self.assertEqual(result["user"]["personal"]["name"], "Jane Doe")
            self.assertEqual(result["metadata"]["version"], "1.0")
        finally:
            if os.path.exists(nested_json_file):
                os.remove(nested_json_file)

    def test_read_empty_json_object(self):
        """Test reading an empty JSON object."""
        empty_data = {}
        empty_json_file = os.path.join(self.temp_dir, "empty_test.json")
        with open(empty_json_file, 'w') as f:
            json.dump(empty_data, f)
        
        try:
            result = read_json(empty_json_file)
            self.assertEqual(result, {})
            self.assertIsInstance(result, dict)
        finally:
            if os.path.exists(empty_json_file):
                os.remove(empty_json_file)

    def test_read_json_with_special_characters(self):
        """Test reading a JSON file with special characters and unicode."""
        special_data = {
            "unicode": "Hello 世界 🌍",
            "special_chars": "!@#$%^&*()_+-=[]{}|;':\",./<>?",
            "escaped": "Line 1\nLine 2\tTabbed",
            "empty_string": "",
            "null_value": None
        }
        
        special_json_file = os.path.join(self.temp_dir, "special_test.json")
        with open(special_json_file, 'w', encoding='utf-8') as f:
            json.dump(special_data, f, ensure_ascii=False)
        
        try:
            result = read_json(special_json_file)
            self.assertEqual(result, special_data)
            self.assertEqual(result["unicode"], "Hello 世界 🌍")
            self.assertIsNone(result["null_value"])
        finally:
            if os.path.exists(special_json_file):
                os.remove(special_json_file)

    def test_file_not_found_error(self):
        """Test that FileNotFoundError is raised for non-existent files."""
        non_existent_file = "/path/to/non/existent/file.json"
        
        with self.assertRaises(FileNotFoundError) as context:
            read_json(non_existent_file)
        
        # Should raise the custom FileNotFoundError message since validation happens first
        self.assertIn("does not exist or is not a JSON file", str(context.exception))

    def test_non_json_file_extension_error(self):
        """Test that FileNotFoundError is raised for files without .json extension."""
        with self.assertRaises(FileNotFoundError) as context:
            read_json(self.non_json_file)
        
        self.assertIn(self.non_json_file, str(context.exception))
        self.assertIn("does not exist or is not a JSON file", str(context.exception))

    def test_invalid_json_content_error(self):
        """Test that json.JSONDecodeError is raised for invalid JSON content."""
        with self.assertRaises(json.JSONDecodeError):
            read_json(self.invalid_json_file)

    def test_empty_file_error(self):
        """Test reading an empty file raises JSONDecodeError."""
        empty_file = os.path.join(self.temp_dir, "empty.json")
        with open(empty_file, 'w') as f:
            pass  # Create empty file
        
        try:
            with self.assertRaises(json.JSONDecodeError):
                read_json(empty_file)
        finally:
            if os.path.exists(empty_file):
                os.remove(empty_file)

    def test_file_with_only_whitespace_error(self):
        """Test reading a file with only whitespace raises JSONDecodeError."""
        whitespace_file = os.path.join(self.temp_dir, "whitespace.json")
        with open(whitespace_file, 'w') as f:
            f.write("   \n\t  \n   ")
        
        try:
            with self.assertRaises(json.JSONDecodeError):
                read_json(whitespace_file)
        finally:
            if os.path.exists(whitespace_file):
                os.remove(whitespace_file)

    def test_file_path_validation_edge_cases(self):
        """Test edge cases for file path validation."""
        # Test file that exists but doesn't end with .json
        with self.assertRaises(FileNotFoundError) as context:
            read_json(self.non_json_file)
        self.assertIn("does not exist or is not a JSON file", str(context.exception))
        
        # Test empty string path - this should raise the custom FileNotFoundError
        with self.assertRaises(FileNotFoundError) as context:
            read_json("")
        self.assertIn("does not exist or is not a JSON file", str(context.exception))
        
        # Test path with .json extension but file doesn't exist
        non_existent_json = "/path/to/missing.json"
        with self.assertRaises(FileNotFoundError) as context:
            read_json(non_existent_json)
        self.assertIn("does not exist or is not a JSON file", str(context.exception))

    def test_validation_logic_comprehensive(self):
        """Test various combinations of file existence and extension validation."""
        # Case 1: File doesn't exist and doesn't end with .json
        non_existent_non_json = "/path/to/missing.txt"
        with self.assertRaises(FileNotFoundError) as context:
            read_json(non_existent_non_json)
        self.assertIn("does not exist or is not a JSON file", str(context.exception))
        
        # Case 2: File exists but doesn't end with .json (already tested in other methods)
        with self.assertRaises(FileNotFoundError):
            read_json(self.non_json_file)
        
        # Case 3: File doesn't exist but ends with .json
        non_existent_json = "/path/to/missing.json"
        with self.assertRaises(FileNotFoundError):
            read_json(non_existent_json)

    def test_json_array_as_root(self):
        """Test reading a JSON file with an array as the root element."""
        array_data = [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
            {"id": 3, "name": "Item 3"}
        ]
        
        array_json_file = os.path.join(self.temp_dir, "array_test.json")
        with open(array_json_file, 'w') as f:
            json.dump(array_data, f)
        
        try:
            # Note: The function returns a dict, but JSON arrays should also be supported
            # This test documents current behavior - might need to be updated if function
            # is modified to handle arrays differently
            result = read_json(array_json_file)
            self.assertEqual(result, array_data)
            self.assertIsInstance(result, list)
        finally:
            if os.path.exists(array_json_file):
                os.remove(array_json_file)


if __name__ == '__main__':
    unittest.main()
