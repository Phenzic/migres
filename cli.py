import argparse
from .core.migrator import MigrationManager
from .models.migration import MigrationConfig

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract and migrate data from MariaDB to Supabase')
    parser.add_argument('--no-download', action='store_true', 
                       help='Process and migrate data without saving files locally')
    args = parser.parse_args()
    
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

if __name__ == "__main__":
    main()