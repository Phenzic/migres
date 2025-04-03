import psycopg2
from typing import Dict, List, Any
import os
from utils.env_loader import load_environment


class PostgresConnector:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        
    def connect(self) -> None:
        """Establish connection to PostgreSQL"""
        self.connection = psycopg2.connect(self.connection_string)
        
    def disconnect(self) -> None:
        """Close the database connection"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            
    def create_tables(self, schema_definitions: Dict[str, Any]) -> None:
        """Create tables based on schema definitions"""
        pass
        
    def insert_data(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 100000) -> bool:
        """Insert data into a table"""
        pass
        
    def apply_constraints(self, constraints_config: Dict[str, Any]) -> None:
        """Apply database constraints"""
        pass

    @classmethod
    def test_connection(cls):
        """Test PostgreSQL connection using connection string from .env file"""
        # Try to load .env from multiple possible locations
        if not load_environment():
            print("Error: Could not find .env file in the current directory")
            return 1
        connection_string = os.getenv("SUPABASE_CONNECTION_STRING")
        
        if not connection_string:
            # Try to load from environment directly in case it was set elsewhere
            print("Error: Missing PostgreSQL configuration in .env file")
            print("Required variable: SUPABASE_CONNECTION_STRING")
            return 1
                
        try:
            print(f"Attempting to connect to PostgreSQL...")
            conn = psycopg2.connect(connection_string)
            
            # Test the connection by executing a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
            
            conn.close()
            print(f"✅ Successfully connected to PostgreSQL!")
            print(f"Server version: {version}")
            return 0
        
        except Exception as e:
            print(f"❌ Failed to connect to PostgreSQL: {str(e)}")
            return 1