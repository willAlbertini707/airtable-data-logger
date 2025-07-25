import os 
import pandas as pd
from datetime import datetime
from pyairtable import Api
from pyairtable.api.base import Base
from typing import List, Dict

class AirtableBaseInterface:
    
    def __init__(self, api_token: str, base_name: str):
        """
        Initializes the AirtableBaseInterface with the API token and base name.

        Args:
            api_token (str): The API token for Airtable.
            base_name (str): The name of the Airtable base.
        """
        self._api = Api(api_token)
        self._base: Base = self._get_base_from_metadata(base_name)

        # get all tables from the base 
        self._table_map = self._build_map_from_tables(self._base.tables())

    def _get_base_from_metadata(self, base_name: str) -> Base:
        """
        Retrieves the Airtable base object from the API using the base name.

        Args:
            base_name (str): The name of the Airtable base.

        Returns:
            Base: The Airtable base object.

        Raises:
            ValueError: If the base with the specified name does not exist.
        """
        # get all bases 
        bases = self._api.bases()

        # serach through bases and find the one with the specified name
        for base in bases:
            if base.name == base_name:
                return base
            
        # if no base found, raise an error
        raise ValueError(f"Base '{base_name}' not found.")
    
    def _build_map_from_tables(self, tables: List[Base]) -> Dict[str, Base]:
        """
        Builds a map from table names to table objects.

        Args:
            tables (List[Base]): List of Airtable table objects.

        Returns:
            Dict[str, Base]: A dictionary mapping table names to table objects.
        """
        return {table.name: table for table in tables}
    
    def read_table(self, name: str) -> pd.DataFrame:
        """
        Retrieve a table from the Airtable base as a DataFrame.

        Args:
            name (str): The name of the table.

        Returns:
            pd.DataFrame: The DataFrame containing the table data.
        """
        # check if the table exists
        if name not in self._table_map:
            raise ValueError(f"Table '{name}' does not exist in the base.")

        # retrieve the table data
        table = self._table_map[name]
        print(f"Reading table: {name}")
        records = table.all()

        for entry in records:
            
            for field, value in entry['fields'].items():
                if field == 'Last Modified':
                    print(f"Field: {field}, Value: {value}")
                    value = self._parse_datetime(value)
                    print(value)
            print('\n\n')

        # convert the records to a DataFrame
        # df = pd.DataFrame.from_records()
        # return df

    @staticmethod
    def _parse_datetime(date_str: str) -> datetime:
        """
        Parses a date string into a datetime object.

        Args:
            date_str (str): The date string to parse.

        Returns:
            datetime: The parsed datetime object.
        """
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    def _write_datetime(self, date: datetime) -> str:
        """
        Converts a datetime object to a string in the format used by Airtable.

        Args:
            date (datetime): The datetime object to convert.

        Returns:
            str: The formatted date string.
        """
        return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")