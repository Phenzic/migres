import argparse
import shutil
from pathlib import Path
from core.migrator import MigrationManager
from models.migration import MigrationConfig
from dotenv import load_dotenv

def main():
    parser = argparse.ArgumentParser(description='Database migration tool')
    parser.add_argument('--init', action='store_true', 
                       help='Generate template config files')
    parser.add_argument('--no-download', action='store_true', 
                       help='Process and migrate data without saving files locally')
    
    args = parser.parse_args()
    
    if args.init:
        init_configs()
        return
        
    # Load environment variables
    if not load_dotenv():
        print("Warning: No .env file found. Using system environment variables")
    
    # Load configuration
    config = MigrationConfig.load_from_files(
        main_config_path="./config.ini",
        type_config_path="./type_config.ini",
        uuid_config_path="./uuid_config.ini",
        schema_config_path="./table_schema.ini",
        constraints_path="./constraints.ini"
    )
    
    # Run migration
    migrator = MigrationManager(config)
    migrator.run_migration()

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
MARIADB_DATABASE3=another_db
SUPABASE_CONNECTION_STRING=postgresql://user:password@host:5432/db
""")
        print("Created .env.example - rename to .env and fill in your credentials")

if __name__ == "__main__":
    main()