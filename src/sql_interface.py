"""
This module provides an interface that allows interaction with sqlite databases.
"""

from database_interface import DatabaseInterface
import sqlite3
import pandas as pd
import os

class SqliteInterface(DatabaseInterface):

    def __init__(self, db_path: str, create_if_not_exists: bool = True) -> None:
        """
        Initializes the SqliteInterface with the path to the SQLite database.

        Args:
            db_path (str): The path to the SQLite database file.
            create_if_not_exists (bool): If True, creates the database file if it does not exist.
        """
        # raise error if file does not exist and create_if_not_exists is False
        if not os.path.isfile(db_path) and not create_if_not_exists:
            raise FileNotFoundError(f"The database file {db_path} does not exist.")
        
        # create the file if it does not exist and create_if_not_exists is True
        elif not os.path.isfile(db_path) and create_if_not_exists:
            dir_path = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
            os.makedirs(dir_path, exist_ok=True)
            open(db_path, 'w').close()
        
        # set path and connect to database
        self._db_path = db_path
        self._connection = sqlite3.connect(db_path)

    def write_table(self, name: str, data: pd.DataFrame, overwrite: bool = False) -> None:
        """
        Write a DataFrame to a table in the SQLite database.

        Args:
            name (str): The name of the table.
            data (pd.DataFrame): The DataFrame to write to the table.
            overwrite (bool): If True, overwrites the existing table.

        Raises:
            ValueError: If the table with the specified name already exists and overwrite is False.
        """
        # write a DataFrame to a table in the SQLite database
        if overwrite:
            data.to_sql(name, self._connection, if_exists='replace', index=False)
        else:
            data.to_sql(name, self._connection, if_exists='fail', index=False)

    def read_table(self, name: str) -> pd.DataFrame:
        """
        Retrieve a table from the SQLite database as a DataFrame.

        Args:
            name (str): The name of the table.

        Returns:
            pd.DataFrame: The DataFrame containing the table data.

        Raises:
            ValueError: If the table with the specified name does not exist.
        """
        # check if the table exists
        if not self._check_table_exists(name):
            raise ValueError(f"Table '{name}' does not exist in the database.")

        # read a table from the SQLite database
        return pd.read_sql_query(f"SELECT * FROM {name}", self._connection)
    
    def add_row(self, name: str, data: dict) -> None:
        pass

    def add_column(self, name: str, column_name: str, column_type: str) -> None:
        pass
    
    def _check_table_exists(self, name: str) -> bool:
        """
        Check if a table exists in the SQLite database.

        Args:
            name (str): The name of the table.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        # build a query to check if the table exists
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}';"

        # execute the query and return True if the table exists, False otherwise
        cursor = self._connection.cursor()
        cursor.execute(query)

        # fetch the result and check if the table exists
        return cursor.fetchone() is not None