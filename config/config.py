import os
import configparser
from pathlib import Path
from typing import Dict, Any
from pydantic import BaseSettings

class ConfigManager:
    def __init__(self):
        self.configs: Dict[str, configparser.ConfigParser] = {}
        
    def load_config(self, config_name: str, config_path: str) -> configparser.ConfigParser:
        """Load a configuration file"""
        config = configparser.ConfigParser()
        config.read(config_path)
        self.configs[config_name] = config
        return config
        
    def get_config(self, config_name: str) -> configparser.ConfigParser:
        """Get a loaded configuration"""
        return self.configs.get(config_name)
        
    def save_config(self, config_name: str, file_path: str) -> None:
        """Save a configuration to file"""
        if config_name in self.configs:
            with open(file_path, 'w') as configfile:
                self.configs[config_name].write(configfile)


class DBSettings(BaseSettings):
    mariadb_host: str = os.getenv("MARIADB_HOST")
    mariadb_user: str = os.getenv("MARIADB_USER")
    mariadb_password: str = os.getenv("MARIADB_PASSWORD")
    mariadb_database1: str = os.getenv("MARIADB_DATABASE1")
    mariadb_database3: str = os.getenv("MARIADB_DATABASE3")
    supabase_connection: str = os.getenv("SUPABASE_CONNECTION_STRING")

    class Config:
        env_file = ".env"