import os
from pathlib import Path

from connectors.mariadb_connector import MariaDBConnector
from models.migration import DatabaseConfig
from utils.env_loader import load_environment

def list_mariadb_tables():
    """List all tables in MariaDB databases"""
    # Load environment variables from default location
    if not load_environment():
        print("Error: Could not find .env file in the current directory")
        return 1
    
    # Get MariaDB connection details from environment
    host = os.getenv("MARIADB_HOST")
    user = os.getenv("MARIADB_USER")
    password = os.getenv("MARIADB_PASSWORD")
    
    if not all([host, user, password]):
        print("Error: Missing MariaDB configuration in .env file")
        print("Required variables: MARIADB_HOST, MARIADB_USER, MARIADB_PASSWORD")
        return 1
    
    # Find all MariaDB database environment variables
    db_vars = [var for var in os.environ if var.startswith("MARIADB_DATABASE")]
    
    if not db_vars:
        print("Error: No MariaDB databases defined in .env file")
        print("Define at least one database with MARIADB_DATABASE1, MARIADB_DATABASE2, etc.")
        return 1
    
    # Connect to each database and list tables
    for db_var in sorted(db_vars):
        db_name = os.getenv(db_var)
        if not db_name:
            continue
        
        print(f"\nDatabase: {db_name}")
        print("-" * 40)
        
        try:
            # Create a temporary config
            db_config = DatabaseConfig(host=host, user=user, password=password, database=db_name)
            connector = MariaDBConnector(db_config)
            connector.connect()
            
            # Get tables
            tables = connector.get_tables()
            
            if not tables:
                print("No tables found")
            else:
                for i, table in enumerate(sorted(tables), 1):
                    print(f"{i}. {table}")
            
            connector.disconnect()
            
        except Exception as e:
            print(f"Error connecting to database {db_name}: {str(e)}")
    
    return 0
