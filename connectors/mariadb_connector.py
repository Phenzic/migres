import pymysql
import pandas as pd
from typing import Dict, Any, List, Optional
from models.migration import DatabaseConfig
import pymysql
from pathlib import Path
import os
from dotenv import load_dotenv

class MariaDBConnector:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        
    def connect(self) -> None:
        """Establish connection to MariaDB"""
        self.connection = pymysql.connect(
            host=self.config.host,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            connect_timeout=28800,
            read_timeout=28800,
            write_timeout=28800,
            charset="utf8mb4"
        )
        
    def disconnect(self) -> None:
        """Close the database connection"""
        if self.connection and self.connection.open:
            self.connection.close()
            
    def read_table(self, table_name: str, columns: List[str], chunk_size: int = 500000) -> pd.DataFrame:
        """Read a table in chunks and return as DataFrame
        
        Args:
            table_name: Name of the table to read
            columns: List of column names to select
            chunk_size: Number of rows to fetch in each chunk
            
        Returns:
            DataFrame containing the table data
        """
        if not self.connection or not self.connection.open:
            self.connect()
            
        # Construct the query
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        
        # Create cursor and execute query
        cursor = self.connection.cursor()
        cursor.execute(query)
        
        # Fetch data in chunks
        data = []
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            data.extend(chunk)
        
        # Close cursor
        cursor.close()
        
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df
        
    def execute_query(self, query: str, params=None) -> Optional[pd.DataFrame]:
        """Execute a SQL query and return results as DataFrame
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            DataFrame containing query results or None for non-SELECT queries
        """
        if not self.connection or not self.connection.open:
            self.connect()
            
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        
        # Check if query returns data (SELECT queries)
        if cursor.description:
            # Get column names
            columns = [col[0] for col in cursor.description]
            
            # Fetch all data
            data = cursor.fetchall()
            
            # Close cursor
            cursor.close()
            
            # Return as DataFrame
            return pd.DataFrame(data, columns=columns)
        else:
            # For non-SELECT queries (INSERT, UPDATE, DELETE)
            self.connection.commit()
            cursor.close()
            return None

    def select_database(self, database_name: str) -> None:
        """
        Switch to a different database
        
        Args:
            database_name: Name of the database to select
        """
        if not self.connection or not self.connection.open:
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(f"USE {database_name}")
        self.connection.commit()
        cursor.close()
        
        # Update the current database name
        self.config.database = database_name

    def get_tables(self) -> List[str]:
        """
        Get all tables in the current database
        
        Returns:
            List of table names
        """
        if not self.connection or not self.connection.open:
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        return tables

    @classmethod
    def test_connection(cls):
        """Test MariaDB connection using credentials from .env file"""
        
        # Try to load .env from multiple possible locations
        env_paths = [
            Path('./.env'),                    # Current directory
            Path('../.env'),                   # Parent directory
            Path.home() / '.env',              # Home directory
            Path(__file__).parent / '.env',    # Script directory
            Path(__file__).parent.parent / '.env'  # Parent of script directory
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
        
        host = os.getenv("MARIADB_HOST")
        user = os.getenv("MARIADB_USER")
        password = os.getenv("MARIADB_PASSWORD")
        database = os.getenv("MARIADB_DATABASE1")
        
        # Check if all required environment variables are set
        if not all([host, user, password, database]):
            print("Error: Missing MariaDB configuration in .env file")
            print("Required variables: MARIADB_HOST, MARIADB_USER, MARIADB_PASSWORD, MARIADB_DATABASE1")
            return 1
        
        try:
            print(f"Attempting to connect to MariaDB at {host}...")
            conn = pymysql.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                connect_timeout=10
            )
            
            # Test the connection by executing a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
            
            conn.close()
            print(f"✅ Successfully connected to MariaDB!")
            print(f"Server version: {version}")
            return 0
        
        except Exception as e:
            print(f"❌ Failed to connect to MariaDB: {str(e)}")
            return 1