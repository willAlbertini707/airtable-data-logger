"""
This file is part of the Airtable project, which provides an interface to interact with Airtable bases.
The database_interface.py file is an abstract class that defines the interface for database operations.
It is designed to be extended by specific database implementations.
It includes methods for connecting to the database, retrieving tables, and handling errors.
"""

from abc import ABC, abstractmethod
import pandas as pd

class DatabaseInterface(ABC):
    """
    Abstract base class for database interfaces.
    This class defines the methods that must be implemented by any database interface.
    """

    @abstractmethod
    def read_table(self, name: str) -> pd.DataFrame:
        """
        Retrieve a list of tables from the database.
        """
        pass

    @abstractmethod
    def write_table(self, name: str, data: pd.DataFrame, overwrite: bool) -> None:
        """
        Write a DataFrame to a table in the database.
        """
        pass

    @abstractmethod
    def add_row(self, name: str, data: dict) -> None:
        """
        Add a row to a table in the database.
        """
        pass

    @abstractmethod
    def add_column(self, name: str, column_name: str, column_type: str) -> None:
        """
        Add a column to a table in the database.
        """
        pass