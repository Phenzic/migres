from typing import Dict, Any, List, Optional
from connectors.mariadb_connector import MariaDBConnector
from connectors.postgres_connector import PostgresConnector
from core.data_processor import DataProcessor
from models.migration import MigrationConfig
import configparser
import os
from config.table_sorter import TableSorter

class MigrationManager:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.mariadb = MariaDBConnector(config.mariadb_config)
        self.postgres = PostgresConnector(config.postgres_config.connection_string)
        self.data_processor = DataProcessor(config.config_manager)
        self.maria_config = self._load_maria_config()
        
    def _load_maria_config(self) -> configparser.ConfigParser:
        """Load MariaDB export configuration"""
        maria_config = configparser.ConfigParser()
        if os.path.exists("maria_config.ini"):
            maria_config.read("maria_config.ini")
        return maria_config
        
    def _get_tables_to_export(self) -> Dict[str, List[str]]:
        """Determine which tables to export based on configuration"""
        export_all = self.maria_config.getboolean("export_settings", "export_all_tables", fallback=True)
        
        # Get list of all tables from MariaDB
        self.mariadb.connect()
        all_tables = {}
        for db_name in self.config.mariadb_databases:
            self.mariadb.select_database(db_name)
            tables = self.mariadb.get_tables()
            all_tables[db_name] = tables
        
        # Apply inclusion/exclusion rules
        result = {}
        for db_name, tables in all_tables.items():
            filtered_tables = []
            
            for table in tables:
                include_table = export_all  # Default based on export_all setting
                
                # Check if table has explicit setting
                if self.maria_config.has_section("tables"):
                    table_setting = self.maria_config.get("tables", table, fallback=None)
                    if table_setting == "include":
                        include_table = True
                    elif table_setting == "exclude":
                        include_table = False
                
                if include_table:
                    filtered_tables.append(table)
            
            if filtered_tables:
                result[db_name] = filtered_tables
                
        return result
        
    def _get_columns_to_export(self, table_name: str) -> List[str]:
        """Determine which columns to export for a table based on configuration"""
        export_all = self.maria_config.getboolean("export_settings", "export_all_columns", fallback=True)
        
        # Get all columns for the table
        all_columns = self.mariadb.get_columns(table_name)
        
        # Apply inclusion/exclusion rules
        filtered_columns = []
        for column in all_columns:
            include_column = export_all  # Default based on export_all setting
            
            # Check if column has explicit setting
            if self.maria_config.has_section("columns"):
                column_key = f"{table_name}.{column}"
                column_setting = self.maria_config.get("columns", column_key, fallback=None)
                if column_setting == "include":
                    include_column = True
                elif column_setting == "exclude":
                    include_column = False
            
            if include_column:
                filtered_columns.append(column)
                
        return filtered_columns
        
    def run(self, no_download: bool = False) -> None:
        """Execute the full migration process"""
        try:
            self.mariadb.connect()
            self.postgres.connect()
            
            # Determine tables to export
            tables_to_export = self._get_tables_to_export()
            
            # Create tables in PostgreSQL
            self.postgres.create_tables(self.config.schema_definitions)
            
            # Process each database
            for db_name, tables in tables_to_export.items():
                self.mariadb.select_database(db_name)
                
                # Use the table sorter to determine migration order
                sorter = TableSorter(self.mariadb, self.postgres, self.maria_config)
                ordered_tables = sorter.get_migration_order(db_name)
                
                # Filter ordered_tables to only include tables we want to export
                ordered_tables = [t for t in ordered_tables if t in tables]
                
                print(f"Migrating tables in optimized order:")
                for i, table in enumerate(ordered_tables, 1):
                    print(f"{i}. {table}")
                
                # Process tables in the determined order
                for table in ordered_tables:
                    columns = self._get_columns_to_export(table)
                    self._process_table(table, columns, no_download)
                    
            # Apply constraints
            self.postgres.apply_constraints(self.config.constraints)
            
        finally:
            self.mariadb.disconnect()
            self.postgres.disconnect()
            
    def _process_table(self, table_name: str, columns: List[str], no_download: bool) -> None:
        """Process a single table
        
        Args:
            table_name: Name of the table to process
            columns: List of columns to export
            no_download: If True, don't save data locally
        """
        print(f"Processing table: {table_name}")
        
        # Read data from MariaDB
        df = self.mariadb.read_table(table_name, columns)
        
        if df.empty:
            print(f"  No data found in table {table_name}")
            return
            
        print(f"  Read {len(df)} rows")
        
        # Process data
        processed_data = self.data_processor.process_table_data(table_name, df)
        
        # Save to file if requested
        if not no_download:
            # Save to file logic here
            pass
        
        # Insert into PostgreSQL
        self.postgres.insert_data(table_name, processed_data)
        print(f"  Inserted {len(processed_data)} rows into PostgreSQL")