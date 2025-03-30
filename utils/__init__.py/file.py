import configparser
from pathlib import Path
from typing import Dict, Any

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