from dataclasses import dataclass
from typing import Dict, List, Any
import configparser

@dataclass
class DatabaseConfig:
    host: str
    user: str
    password: str
    database: str
    connect_timeout: int = 28800
    read_timeout: int = 28800
    write_timeout: int = 28800
    charset: str = "utf8mb4"

@dataclass
class PostgresConfig:
    connection_string: str

@dataclass
class MigrationConfig:
    mariadb_config: DatabaseConfig
    postgres_config: PostgresConfig
    tables_to_export: Dict[str, List[str]]
    columns_to_export: Dict[str, List[str]]
    schema_definitions: Dict[str, Any]
    type_conversions: Dict[str, Any]
    uuid_config: Dict[str, Any]
    constraints: Dict[str, Any]
    
    @classmethod
    def load_from_files(cls, main_config_path: str, type_config_path: str,
                       uuid_config_path: str, schema_config_path: str,
                       constraints_path: str) -> 'MigrationConfig':
        """Create config from ini files"""
        # Implementation to load all config files
        pass