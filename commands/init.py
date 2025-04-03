from pathlib import Path


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
MARIADB_DATABASE1=source_db
SUPABASE_CONNECTION_STRING=postgresql://user:password@host:5432/db
""")
        print("Created .env.example - rename to .env and fill in your credentials")
