import os 
import pandas as pd
from datetime import datetime, timedelta
from pyairtable import Api
from pyairtable.api.base import Base
from pyairtable.api.table import Table
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
        self._table_map: Dict[str, Table] = self._build_map_from_tables(self._base.tables())

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
        The table contains all records with their fields and unique IDs.

        Args:
            name (str): The name of the table.

        Returns:
            pd.DataFrame: The DataFrame containing the table data.
        """
        # create dictionary to hold table records
        record_list = []

        # check if the table exists
        if name not in self._table_map:
            raise ValueError(f"Table '{name}' does not exist in the base.")

        # retrieve the table data
        table = self._table_map[name]
        records = table.all()

        for record in records:

            # take the unique ID from the record
            id = record['id']

            # take the fields from the record
            fields = record['fields']

            # add unique ID to the fields
            fields['id'] = id

            # add the record to the list
            record_list.append(fields)

        # convert the records to a DataFrame
        df = pd.DataFrame(record_list)

        return df
    
    def read_all_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Retrieve all tables from the Airtable base as a dictionary of DataFrames.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary mapping table names to DataFrames.
        """
        # create a dictionary to hold all tables
        all_tables = {}

        # iterate through all tables and read them
        for name in self._table_map.keys():
            all_tables[name] = self.read_table(name)

        return all_tables