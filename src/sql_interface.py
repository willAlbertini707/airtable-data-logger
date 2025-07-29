"""
This module provides an interface that allows interaction with sqlite databases.
"""

from database_interface import DatabaseInterface
import sqlite3
import pandas as pd
import os
from typing import Literal, List, Dict, Any

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

    def write_table(self, table_name: str, data: pd.DataFrame, if_exists: Literal['fail', 'replace', 'append'] = 'fail') -> None:
        """
        Write a DataFrame to a table in the SQLite database.

        Args:
            table_name (str): The name of the table.
            data (pd.DataFrame): The DataFrame to write to the table.
            overwrite (bool): If True, overwrites the existing table.

        Raises:
            ValueError: If the table with the specified name already exists and overwrite is False.
        """
        # write a DataFrame to a table in the SQLite database
        data.to_sql(table_name, self._connection, if_exists=if_exists, index=False)

    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Retrieve a table from the SQLite database as a DataFrame.

        Args:
            table_name (str): The name of the table.

        Returns:
            pd.DataFrame: The DataFrame containing the table data.

        Raises:
            ValueError: If the table with the specified name does not exist.
        """
        # check if the table exists
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist in the database.")

        # read a table from the SQLite database
        return pd.read_sql_query(f"SELECT * FROM {table_name}", self._connection)

    def add_row(self, table_name: str, data: Dict[str, Any]) -> None:
        """
        Adds a row to a table in an SQLite database.

        Args:
            table_name (str): Name of table to add row to
            data (dict): Data to add to table, where keys are column names and values are the data to insert.
        """
        # check if the table exists
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # build the insert query
        query = f'INSERT INTO {table_name} (' + ','.join(data.keys()) + ')'
        query += ' VALUES (' + ','.join(data.values()) + ')'
        
        # execute the query
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()

    def add_column(self, table_name: str, column_name: str, column_type: str) -> None:
        """
        This method adds a column to a table in an SQLite database.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column to add.
            column_type (str): The data type of the column to add.
        """
        # check if the table exists
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")

        # build the alter table query
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"

        # execute the query
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()

    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the SQLite database.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        # build a query to check if the table exists
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"

        # execute the query and return True if the table exists, False otherwise
        cursor = self._connection.cursor()
        cursor.execute(query)

        # fetch the result and check if the table exists
        return cursor.fetchone() is not None
    
    def _check_columns(self, table_name: str, columns: List[str]) -> List[bool]:
        """
        Check if the specified columns exist in the table.

        Args:
            table_name (str): The name of the table.
            columns (List[str]): List of column names to check.

        Returns:
            List[bool]: A list of booleans indicating whether each column exists in the table.
        """
        # check if the table exists
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")

        # build a query to get the column names
        query = f"PRAGMA table_info({table_name});"
        
        # execute the query and fetch the results
        cursor = self._connection.cursor()
        cursor.execute(query)
        existing_columns = [row[1] for row in cursor.fetchall()]

        # check if all specified columns exist
        return [col in existing_columns for col in columns]
    
    def check_and_add_columns(self, table_name: str, columns: List[str], column_types: List[str]) -> None:
        """
        Check if the specified columns exist in the table and add them if they do not.

        Args:
            table_name (str): The name of the table.
            columns (List[str]): The names of the columns to check/add.
            column_types (List[str]): The data types of the columns to add.
        """
        # check if the table exists
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")

        # check if the columns exist
        existing_columns = self._check_columns(table_name, columns)

        # add any missing columns
        for col, col_type, exists in zip(columns, column_types, existing_columns):
            if not exists:
                self.add_column(table_name, col, col_type)
    
    def upsert_batch(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Upserts a DataFrame into a table in the SQLite database.
        Note: The table must have a PRIMARY KEY or UNIQUE constraint on 'id' column for upsert to work.

        Args:
            table_name (str): The name of the table.
            data (pd.DataFrame): The DataFrame to upsert into the table.
        """
        # check if the table exists
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")

        # create data batch for execute many
        data_batch = [tuple(row) for row in data.to_numpy()]

        # build the upsert query with dynamic placeholders
        columns = ', '.join(data.columns)
        placeholders = ', '.join(['?' for _ in data.columns])
        update_columns = [f"{col}=excluded.{col}" for col in data.columns if col != 'id']
        
        if not update_columns:
            # If only id column, just insert or ignore
            query = f"""
            INSERT OR IGNORE INTO {table_name} ({columns})
            VALUES ({placeholders})
            """
        else:
            query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT(id) DO UPDATE SET
            {', '.join(update_columns)}
            """

        # execute the query
        cursor = self._connection.cursor()
        cursor.executemany(query, data_batch)
        self._connection.commit()
