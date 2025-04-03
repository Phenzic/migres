import argparse
import sys
from pathlib import Path
from dotenv import load_dotenv
import os

from commands.init import init_configs
from commands.test import test_connection
from commands.sort import sort_tables
from commands.run import run_migration
from commands.list import list_mariadb_tables
from commands.exclude import (
    run_migration_with_exclusions,
    run_migration_with_column_exclusions
)

__version__ = "0.1.1.dev1"  # Match the version in pyproject.toml

def main():
    # Load environment variables at the very beginning
    load_dotenv()
    
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
    run_parser.add_argument('--no-download', action='store_true', 
                           help='Process and migrate data without saving files locally')
    # Sort command
    sort_parser = subparsers.add_parser('sort', help='Determine optimal table migration order')
    
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
        print("Initializing configuration files...")
        return init_configs()
    elif args.command == 'run':
        return run_migration(args.no_download if hasattr(args, 'no_download') else False)
    elif args.command == 'sort':
        return sort_tables()
    else:
        # If no command is provided, show version
        print(f"migres version {__version__}")
        print("A database migration tool")
        return 0

if __name__ == "__main__":
    sys.exit(main())