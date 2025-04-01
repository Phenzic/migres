import argparse
import shutil
from pathlib import Path
from core.migrator import MigrationManager
from models.migration import MigrationConfig
from dotenv import load_dotenv
import os
import sys
import configparser
from config.table_sorter import TableSorter

__version__ = "0.1.0.dev1"  # Match the version in pyproject.toml

def main():
    parser = argparse.ArgumentParser(description='Database migration tool')
    parser.add_argument('--version', '-v', action='store_true', help='Show version information')
    parser.add_argument('--test', '-t', nargs='?', const='all', choices=['all', 'maria', 'postgres'], 
                        help='Test database connection (all, maria or postgres)')
    parser.add_argument('--maria-table', choices=['ls'], help='List all tables in MariaDB')
    parser.add_argument('--maria-exclude', type=str, help='Comma-separated list of tables to exclude from migration')
    parser.add_argument('--maria-exclude-columns', type=str, 
                        help='Comma-separated list of columns to exclude in format "table.column"')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize configuration files')
    # Run command
    run_parser = subparsers.add_parser('run', help='Run the migration')
    sort_parser = subparsers.add_parser('sort', help='Determine optimal table migration order')
    
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
        if args.test == 'all':
            # Test all connections
            maria_result = test_connection('maria')
            postgres_result = test_connection('postgres')
            return 1 if maria_result == 1 or postgres_result == 1 else 0
        else:
            return test_connection(args.test)
    
    # Handle listing MariaDB tables
    if args.maria_table == 'ls':
        return list_mariadb_tables()
    
    # Handle excluding tables and columns - process both if present
    tables_excluded = False
    columns_excluded = False
    
    if args.maria_exclude:
        run_migration_with_exclusions(args.maria_exclude)
        tables_excluded = True
        
    if args.maria_exclude_columns:
        run_migration_with_column_exclusions(args.maria_exclude_columns)
        columns_excluded = True
        
    if tables_excluded or columns_excluded:
        return 0
    
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
    elif args.command == 'sort':
        return sort_tables()
    else:
        # If no command is provided, show version
        print(f"migres version {__version__}")
        print("A database migration tool")
        return 0

def list_mariadb_tables():
    """List all tables in MariaDB databases"""
    from connectors.mariadb_connector import MariaDBConnector
    from models.migration import DatabaseConfig
    
    load_dotenv()
    
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
        print("Define at least one database with MARIADB_DATABASE, MARIADB_DATABASE2, etc.")
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

def run_migration_with_exclusions(exclusions, no_download=False):
    """Run migration with table exclusions"""
    # Parse the comma-separated list, handling spaces
    if exclusions.startswith('"') and exclusions.endswith('"'):
        exclusions = exclusions[1:-1]  # Remove surrounding quotes
    
    excluded_tables = [table.strip() for table in exclusions.split(',')]
    
    # Check if maria_config.ini exists
    if os.path.exists("maria_config.ini"):
        # Read the file as text to preserve existing content
        with open("maria_config.ini", "r") as f:
            content = f.read()
        
        # Check if [tables] section exists
        if "[tables]" not in content:
            # Add [tables] section if it doesn't exist
            content += "\n[tables]\n"
        else:
            # Find the position of [tables] section
            tables_pos = content.find("[tables]")
            next_section_pos = content.find("[", tables_pos + 1)
            
            # If there's another section after [tables]
            if next_section_pos != -1:
                # Insert our entries before the next section
                section_content = content[tables_pos:next_section_pos]
                rest_content = content[next_section_pos:]
                
                # Add our entries to the section
                for table in excluded_tables:
                    if table and table.strip() not in section_content:
                        section_content += f"{table.strip()}\n"
                
                # Combine everything
                content = content[:tables_pos] + section_content + rest_content
            else:
                # [tables] is the last section, append to the end
                for table in excluded_tables:
                    if table and table.strip() not in content:
                        content += f"{table.strip()}\n"
    else:
        # Create a new file with proper format
        content = """
[tables]
"""
        # Add excluded tables
        for table in excluded_tables:
            if table:  # Skip empty strings
                content += f"{table.strip()}\n"
        
        # Add columns section
        content += """
[columns]
"""
    
    # Write the updated content back to the file
    with open("maria_config.ini", "w") as f:
        f.write(content)
    
    print(f"Successfully appended excluded tables: {', '.join(excluded_tables)}")
    
    # Don't run the migration, just update the config
    return 0

def run_migration_with_column_exclusions(exclusions, no_download=False):
    """Run migration with column exclusions"""
    # Parse the comma-separated list, handling spaces
    if exclusions.startswith('"') and exclusions.endswith('"'):
        exclusions = exclusions[1:-1]  # Remove surrounding quotes
    
    excluded_columns = [column.strip() for column in exclusions.split(',')]
    
    # Check if maria_config.ini exists
    if os.path.exists("maria_config.ini"):
        # Read the file as text to preserve existing content
        with open("maria_config.ini", "r") as f:
            content = f.read()
        
        # Check if [columns] section exists
        if "[columns]" not in content:
            # Add [columns] section if it doesn't exist
            content += "\n[columns]\n"
        else:
            # Find the position of [columns] section
            columns_pos = content.find("[columns]")
            next_section_pos = content.find("[", columns_pos + 1)
            
            # If there's another section after [columns]
            if next_section_pos != -1:
                # Insert our entries before the next section
                section_content = content[columns_pos:next_section_pos]
                rest_content = content[next_section_pos:]
                
                # Add our entries to the section
                for column in excluded_columns:
                    if column and column.strip() not in section_content:
                        section_content += f"{column.strip()}\n"
                
                # Combine everything
                content = content[:columns_pos] + section_content + rest_content
            else:
                # [columns] is the last section, append to the end
                for column in excluded_columns:
                    if column and column.strip() not in content:
                        content += f"{column.strip()}\n"
    else:
        # Create a new file with proper format
        content = """
[tables]


[columns]
"""
        # Add excluded columns
        for column in excluded_columns:
            if column:  # Skip empty strings
                content += f"{column.strip()}\n"
    
    # Write the updated content back to the file
    with open("maria_config.ini", "w") as f:
        f.write(content)
    
    print(f"Successfully appended excluded columns: {', '.join(excluded_columns)}")
    
    # Don't run the migration, just update the config
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
    database = os.getenv("MARIADB_DATABASE")
    
    # Check if all required environment variables are set
    if not all([host, user, password, database]):
        print("Error: Missing MariaDB configuration in .env file")
        print("Required variables: MARIADB_HOST, MARIADB_USER, MARIADB_PASSWORD, MARIADB_DATABASE")
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
        
        "maria_config.ini": """

[tables]
logs_table
temp_data

[columns]
users.password_hash
users.session_token
posts.internal_id

[migration]
# Tables that must be migrated first despite foreign key relationships
force_early = audit_logs
# Tables that must be migrated last
force_late = temp_data
# Custom ordering for specific tables (higher priority tables first)
custom_order = categories, tags, comments
""",
        
        "table_schema.ini": """
# Table Schema Configuration
# Defines the structure and constraints for PostgreSQL tables

[posts]
# Column definitions
id = UUID PRIMARY KEY
user_id = UUID
name = TEXT
content = TEXT
created_at = TIMESTAMP
# Constraints
primary_key = id
foreign_keys = 
    user_id -> auth.users(id)
indexes = created_at

[videos]
# Column definitions
id = UUID PRIMARY KEY
name = TEXT
duration = FLOAT
# Constraints
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

    # Create .env
    if not Path(".env").exists() and not Path(".env.example").exists():
        with open(".env.example", 'w') as f:
            f.write("""# Database Connections
MARIADB_HOST=localhost
MARIADB_USER=root
MARIADB_PASSWORD=yourpassword
MARIADB_DATABASE=source_db
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

def sort_tables():
    """Sort tables based on their dependencies using topology sort"""
    from connectors.mariadb_connector import MariaDBConnector
    from connectors.postgres_connector import PostgresConnector
    from models.migration import DatabaseConfig
    import configparser
    
    load_dotenv()
    
    # Get MariaDB connection details from environment
    host = os.getenv("MARIADB_HOST")
    user = os.getenv("MARIADB_USER")
    password = os.getenv("MARIADB_PASSWORD")
    database = os.getenv("MARIADB_DATABASE1")  # Using MARIADB_DATABASE1 as in your .env
    
    if not all([host, user, password, database]):
        print("Error: Missing MariaDB configuration in .env file")
        print("Required variables: MARIADB_HOST, MARIADB_USER, MARIADB_PASSWORD, MARIADB_DATABASE1")
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
        
        # Print the results
        print("\nOptimal migration order:")
        print("-" * 40)
        for i, table in enumerate(migration_order, 1):
            print(f"{i}. {table}")
        
        # Always save to config
        print("\nSaving migration order to maria_config.ini...")
        sorter.log_migration_order(migration_order)
        print("Done! Migration order has been saved.")
        
        return 0
        
    except Exception as e:
        print(f"Error analyzing database: {str(e)}")
        return 1
    finally:
        mariadb_connector.disconnect()

if __name__ == "__main__":
    sys.exit(main())