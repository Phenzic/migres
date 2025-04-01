import argparse
import shutil
from pathlib import Path
from core.migrator import MigrationManager
from models.migration import MigrationConfig
from dotenv import load_dotenv
import os
import sys

__version__ = "0.1.0.dev1"  # Match the version in pyproject.toml

def main():
    parser = argparse.ArgumentParser(description='Database migration tool')
    parser.add_argument('--version', '-v', action='store_true', help='Show version information')
    parser.add_argument('--test', '-t', nargs='?', const='both', choices=['both', 'maria', 'postgres'], 
                        help='Test database connection (both, maria or postgres)')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize configuration files')
    # Run command
    run_parser = subparsers.add_parser('run', help='Run the migration')
    
    run_parser.add_argument('--no-download', action='store_true', 
                           help='Process and migrate data without saving files locally')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Check for version flag first
    if hasattr(args, 'version') and args.version:
        print(f"migres version {__version__}")
        return 0
    
    # Handle test connection command
    if args.test:
        if args.test == 'both':
            # Test both connections
            maria_result = test_connection('maria')
            postgres_result = test_connection('postgres')
            return 1 if maria_result == 1 or postgres_result == 1 else 0
        else:
            return test_connection(args.test)
    
    # Handle commands
    if args.command == 'init':
        # Your initialization logic here
        print("Initializing configuration files...")
        init_configs()
        return 0
    elif args.command == 'run':
        # Your migration logic here
        config = load_config()
        if not config or not hasattr(config, 'mariadb_config') or config.mariadb_config is None:
            print("Error: MariaDB configuration is missing or invalid.")
            print("Please check your configuration or run 'migres init' to create a new configuration.")
            return 1
        
        migrator = MigrationManager(config)
        migrator.run(no_download=getattr(args, 'no_download', False))
        return 0
    else:
        # If no command is provided, show version
        print(f"migres version {__version__}")
        print("A database migration tool")
        return 0

def test_connection(db_type):
    """Test database connection based on .env configuration
    
    Args:
        db_type: Type of database to test ('maria' or 'postgres')
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    load_dotenv()
    
    if db_type == 'maria':
        return test_mariadb_connection()
    elif db_type == 'postgres':
        return test_postgres_connection()
    else:
        print(f"Unknown database type: {db_type}")
        return 1

def test_mariadb_connection():
    """Test MariaDB connection using credentials from .env file"""
    import pymysql
    
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

def test_postgres_connection():
    """Test PostgreSQL connection using connection string from .env file"""
    import psycopg2
    
    connection_string = os.getenv("SUPABASE_CONNECTION_STRING")
    
    if not connection_string:
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

def init_configs():
    """Create template config files in current directory"""
    config_templates = {
        "type_config.ini": """
[posts]
id = uuid
created_at = timestamp

[videos]
duration = float
censor = boolean
""",
        
        "uuid_config.ini": """
[posts]
id = post

[videos]  
id = video
user_id = user
""",
        
        "table_schema.ini": """
[posts]
id = UUID PRIMARY KEY
user_id = UUID
name = TEXT
content = TEXT
created_at = TIMESTAMP

[videos]
id = UUID PRIMARY KEY
name = TEXT
duration = FLOAT
""",
        
        "constraints.ini": """
[posts]
primary_key = id
foreign_keys = 
    user_id -> auth.users(id)
indexes = created_at

[videos]
primary_key = id
"""
    }

    for filename, content in config_templates.items():
        if not Path(filename).exists():
            with open(filename, 'w') as f:
                f.write(content.strip())
            print(f"Created {filename}")
        else:
            print(f"{filename} already exists - skipping")

    # Create .env.example if it doesn't exist
    if not Path(".env").exists() and not Path(".env.example").exists():
        with open(".env.example", 'w') as f:
            f.write("""# Database Connections
MARIADB_HOST=localhost
MARIADB_USER=root
MARIADB_PASSWORD=yourpassword
MARIADB_DATABASE1=source_db
SUPABASE_CONNECTION_STRING=postgresql://user:password@host:5432/db
""")
        print("Created .env.example - rename to .env and fill in your credentials")

def load_config():
    """Load configuration from environment variables"""
    from config.config import DBSettings
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Create a config object with database settings
    db_settings = DBSettings()
    
    # Create a migration config object
    from models.migration import MigrationConfig
    config = MigrationConfig()
    config.mariadb_config = db_settings
    
    return config

if __name__ == "__main__":
    sys.exit(main())