from typing import Dict, Any
from ..connectors.mariadb_connector import MariaDBConnector
from ..connectors.postgres_connector import PostgresConnector
from ..core.data_processor import DataProcessor
from ..models.migration import MigrationConfig


class MigrationManager:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.mariadb = MariaDBConnector(config.mariadb_config)
        self.postgres = PostgresConnector(config.postgres_config.connection_string)
        self.data_processor = DataProcessor(config.config_manager)
        
    def run_migration(self) -> None:
        """Execute the full migration process"""
        try:
            self.mariadb.connect()
            self.postgres.connect()
            
            # Create tables in PostgreSQL
            self.postgres.create_tables(self.config.schema_definitions)
            
            # Process each table
            for db_name, tables in self.config.tables_to_export.items():
                self.mariadb.select_database(db_name)
                for table in tables:
                    self._process_table(table)
                    
            # Apply constraints
            self.postgres.apply_constraints(self.config.constraints)
            
        finally:
            self.mariadb.disconnect()
            self.postgres.disconnect()
            
    def _process_table(self, table_name: str) -> None:
        """Process a single table"""
        pass