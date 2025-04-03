import os
import configparser
from pathlib import Path
from dotenv import load_dotenv

from connectors.mariadb_connector import MariaDBConnector
from connectors.postgres_connector import PostgresConnector
from config.table_sorter import TableSorter
from models.migration import DatabaseConfig

def sort_tables():
    """Sort tables based on their dependencies using topology sort"""
    load_dotenv()
    
    # Try to load .env from multiple possible locations
    env_paths = [
        Path('./.env'),                    # Current directory
        Path('../.env'),                   # Parent directory
        Path.home() / '.env',              # Home directory
        Path(__file__).parent.parent / '.env',    # Script directory
        Path(__file__).parent.parent.parent / '.env'  # Parent of script directory
    ]
    
    env_loaded = False
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            print(f"Loaded environment from {env_path}")
            env_loaded = True
            break
    
    if not env_loaded:
        print("Warning: No .env file found in common locations")
    
    # Get MariaDB connection details from environment
    host = os.getenv("MARIADB_HOST")
    user = os.getenv("MARIADB_USER")
    password = os.getenv("MARIADB_PASSWORD")
    
    # Try multiple possible database environment variables
    database = None
    for var in ["MARIADB_DATABASE1", "MARIADB_DATABASE", "MARIADB_DB"]:
        database = os.getenv(var)
        if database:
            break
    
    if not all([host, user, password, database]):
        print("Error: Missing MariaDB configuration in .env file")
        print(f"Current values: host={host}, user={user}, password={'*****' if password else None}, database={database}")
        print("Required variables: MARIADB_HOST, MARIADB_USER, MARIADB_PASSWORD, and one of MARIADB_DATABASE1, MARIADB_DATABASE")
        return 1
    
    # Create a temporary config
    db_config = DatabaseConfig(host=host, user=user, password=password, database=database)
    mariadb_connector = MariaDBConnector(db_config)
    
    # Create a dummy postgres connector (not actually used for connections)
    postgres_connector = PostgresConnector("dummy")
    
    # Load maria_config.ini
    maria_config = configparser.ConfigParser(allow_no_value=True)
    if os.path.exists("maria_config.ini"):
        maria_config.read("maria_config.ini")
    
    try:
        # Connect to MariaDB
        mariadb_connector.connect()
        
        # Create table sorter
        sorter = TableSorter(mariadb_connector, postgres_connector, maria_config)
        
        # Get migration order
        print(f"Analyzing database '{database}' for optimal migration order...")
        migration_order = sorter.get_migration_order(database)
        
        # Always save to config
        print("\nSaving migration order to maria_config.ini...")
        sorter.log_migration_order(migration_order)
        print("Done! Migration order has been saved.")
        
        return 0
        
    except Exception as e:
        print(f"Error analyzing database: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        mariadb_connector.disconnect()