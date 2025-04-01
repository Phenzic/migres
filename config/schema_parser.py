import configparser
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

def parse_table_schema():
    """
    Parse table schema configuration including column definitions and constraints
    
    Returns:
        dict: Table schema with columns and constraints
    """
    config = configparser.ConfigParser()
    config_path = Path("table_schema.ini")
    
    if not config_path.exists():
        return {}
    
    config.read(config_path)
    
    schema = {}
    
    for table in config.sections():
        schema[table] = {
            "columns": {},
            "primary_key": None,
            "foreign_keys": [],
            "indexes": []
        }
        
        for key, value in config[table].items():
            # Handle special constraint keys
            if key == "primary_key":
                schema[table]["primary_key"] = value
            elif key == "foreign_keys":
                # Parse multi-line foreign key definitions
                for fk_def in value.strip().split('\n'):
                    if fk_def.strip():
                        schema[table]["foreign_keys"].append(fk_def.strip())
            elif key == "indexes":
                schema[table]["indexes"] = [idx.strip() for idx in value.split(',')]
            else:
                # This is a column definition
                schema[table]["columns"][key] = value
                
                # Check if column definition includes PRIMARY KEY
                if "PRIMARY KEY" in value.upper():
                    schema[table]["primary_key"] = key
                
                # Check if column definition includes REFERENCES (foreign key)
                if "REFERENCES" in value.upper():
                    schema[table]["foreign_keys"].append(f"{key} -> {value.split('REFERENCES')[1].strip()}")
    
    return schema 

class SchemaParser:
    """Parser for table schema and constraints configuration"""
    
    def __init__(self, schema_file_path: str):
        self.config = configparser.ConfigParser()
        self.config.read(schema_file_path)
        
    def get_tables(self) -> List[str]:
        """Get all table names defined in the schema"""
        return [section for section in self.config.sections()]
        
    def get_column_definitions(self, table_name: str) -> Dict[str, str]:
        """Get column definitions for a table
        
        Returns:
            Dictionary mapping column names to their PostgreSQL data types
        """
        if not self.config.has_section(table_name):
            return {}
            
        column_defs = {}
        for key, value in self.config.items(table_name):
            # Skip special keys that define constraints
            if key not in ['primary_key', 'foreign_keys', 'indexes', 'unique']:
                column_defs[key] = value
                
        return column_defs
        
    def get_primary_key(self, table_name: str) -> Optional[str]:
        """Get primary key for a table"""
        if not self.config.has_section(table_name):
            return None
            
        return self.config.get(table_name, 'primary_key', fallback=None)
        
    def get_foreign_keys(self, table_name: str) -> List[Tuple[str, str]]:
        """Get foreign key constraints for a table
        
        Returns:
            List of tuples (column_name, reference) where reference is in format "table(column)"
        """
        if not self.config.has_section(table_name):
            return []
            
        fk_str = self.config.get(table_name, 'foreign_keys', fallback='')
        if not fk_str:
            return []
            
        foreign_keys = []
        for line in fk_str.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
                
            parts = line.split('->')
            if len(parts) != 2:
                continue
                
            column = parts[0].strip()
            reference = parts[1].strip()
            foreign_keys.append((column, reference))
            
        return foreign_keys
        
    def get_indexes(self, table_name: str) -> List[str]:
        """Get indexed columns for a table"""
        if not self.config.has_section(table_name):
            return []
            
        indexes_str = self.config.get(table_name, 'indexes', fallback='')
        if not indexes_str:
            return []
            
        return [idx.strip() for idx in indexes_str.split(',')]
        
    def get_unique_constraints(self, table_name: str) -> List[str]:
        """Get unique constraints for a table"""
        if not self.config.has_section(table_name):
            return []
            
        unique_str = self.config.get(table_name, 'unique', fallback='')
        if not unique_str:
            return []
            
        return [constraint.strip() for constraint in unique_str.split(',')]
        
    def generate_create_table_sql(self, table_name: str) -> str:
        """Generate SQL to create the table with all constraints"""
        if not self.config.has_section(table_name):
            return ""
            
        column_defs = self.get_column_definitions(table_name)
        if not column_defs:
            return ""
            
        # Start building the SQL
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        
        # Add columns
        columns = []
        for col_name, col_type in column_defs.items():
            columns.append(f"    {col_name} {col_type}")
            
        # Add primary key if defined separately
        pk = self.get_primary_key(table_name)
        if pk and "PRIMARY KEY" not in " ".join(column_defs.values()):
            columns.append(f"    PRIMARY KEY ({pk})")
            
        # Add foreign keys
        for col, ref in self.get_foreign_keys(table_name):
            columns.append(f"    FOREIGN KEY ({col}) REFERENCES {ref}")
            
        # Add unique constraints
        for constraint in self.get_unique_constraints(table_name):
            columns.append(f"    UNIQUE ({constraint})")
            
        sql += ",\n".join(columns)
        sql += "\n);"
        
        # Add indexes (these are created separately after the table)
        index_sql = ""
        for idx in self.get_indexes(table_name):
            index_name = f"idx_{table_name}_{idx}"
            index_sql += f"\nCREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({idx});"
            
        if index_sql:
            sql += index_sql
            
        return sql 