import argparse
import shutil
from pathlib import Path
from core.migrator import MigrationManager
from models.migration import MigrationConfig
from dotenv import load_dotenv

__version__ = "0.1.0.dev1"  # Match the version in pyproject.toml

def main():
    parser = argparse.ArgumentParser(description='Database migration tool')
    parser.add_argument('--version', '-v', action='store_true', help='Show version information')
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
    main()