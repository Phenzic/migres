import pandas as pd
import uuid
from typing import Dict, Any, List
from config.config import ConfigManager

class DataProcessor:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
    def convert_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Convert column types based on configuration"""
        pass
        
    def convert_uuids(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Convert IDs to UUIDs"""
        pass
        
    def clean_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Clean and prepare data for insertion"""
        pass
        
    def process_table_data(self, table_name: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Full processing pipeline for table data"""
        pass