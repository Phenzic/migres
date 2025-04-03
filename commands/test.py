from dotenv import load_dotenv
from connectors.mariadb_connector import MariaDBConnector
from connectors.postgres_connector import PostgresConnector

def test_connection(db_type):
    """Test database connection based on .env configuration
    
    Args:
        db_type: Type of database to test ('maria' or 'postgres')
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    load_dotenv()
    
    if db_type == 'maria':
        return MariaDBConnector.test_connection()
    elif db_type == 'postgres':
        return PostgresConnector.test_connection()
    else:
        print(f"Unknown database type: {db_type}")
        return 1
