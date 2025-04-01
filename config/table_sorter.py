import logging
import os
from typing import Dict, List, Set, Tuple
import configparser

class TableSorter:
    """
    Handles the sorting of tables for migration based on their dependencies.
    Uses a topological sort algorithm to determine the correct order.
    """
    
    def __init__(self, mariadb_connector, postgres_connector, maria_config):
        self.mariadb = mariadb_connector
        self.postgres = postgres_connector
        self.maria_config = maria_config
        self.logger = logging.getLogger(__name__)
    
    def get_migration_order(self, db_name: str) -> List[str]:
        """
        Determine the order in which tables should be migrated using topology sort.
        
        Args:
            db_name: The database name to analyze
                
        Returns:
            List of table names in the order they should be migrated
        """
        # Get all tables
        try:
            self.mariadb.select_database(db_name)
            all_tables = self.mariadb.get_tables()
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}")
            # Fallback: try to get tables with a direct query
            query = "SHOW TABLES"
            result = self.mariadb.execute_query(query)
            if result is not None and not result.empty:
                all_tables = result.iloc[:, 0].tolist()
            else:
                self.logger.error("Could not retrieve tables from database")
                return []
        
        # Get configuration overrides
        force_early = self._get_force_early_tables()
        force_late = self._get_force_late_tables()
        custom_order = self._get_custom_order_tables()
        
        # Remove tables that have explicit ordering from topology sort
        tables_to_sort = [t for t in all_tables if t not in force_early and t not in force_late 
                        and t not in custom_order]
        
        # Get dependency graph
        dependencies = self._build_dependency_graph(db_name, tables_to_sort)
        
        # Perform topological sort
        sorted_tables = self._topological_sort(tables_to_sort, dependencies)
        
        # Combine all parts in the correct order
        final_order = force_early + sorted_tables + custom_order + force_late
        
        # Log the migration order
        self.log_migration_order(final_order)
        
        return final_order
        
    def _get_force_early_tables(self) -> List[str]:
        """Get tables that should be migrated first"""
        if not self.maria_config.has_section('migration'):
            return []
        
        force_early = self.maria_config.get('migration', 'force_early', fallback='')
        return [t.strip() for t in force_early.split(',') if t.strip()]
    
    def _get_force_late_tables(self) -> List[str]:
        """Get tables that should be migrated last"""
        if not self.maria_config.has_section('migration'):
            return []
        
        force_late = self.maria_config.get('migration', 'force_late', fallback='')
        return [t.strip() for t in force_late.split(',') if t.strip()]
    
    def _get_custom_order_tables(self) -> List[str]:
        """Get tables with custom ordering"""
        if not self.maria_config.has_section('migration'):
            return []
        
        custom_order = self.maria_config.get('migration', 'custom_order', fallback='')
        return [t.strip() for t in custom_order.split(',') if t.strip()]
    
    def _build_dependency_graph(self, db_name: str, tables: List[str]) -> Dict[str, Set[str]]:
        """
        Build a graph of table dependencies based on foreign keys.
        
        Returns:
            Dictionary mapping table names to sets of tables they depend on
        """
        dependencies = {table: set() for table in tables}
        
        # Query to get foreign key relationships
        query = """
        SELECT 
            TABLE_NAME, REFERENCED_TABLE_NAME
        FROM 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE 
            REFERENCED_TABLE_SCHEMA = %s
            AND TABLE_SCHEMA = %s
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        
        result = self.mariadb.execute_query(query, (db_name, db_name))
        
        if result is not None and not result.empty:
            for _, row in result.iterrows():
                table = row['TABLE_NAME']
                referenced_table = row['REFERENCED_TABLE_NAME']
                
                # Only include tables that are in our list to sort
                if table in tables and referenced_table in tables:
                    dependencies[table].add(referenced_table)
        
        return dependencies
    
    def _topological_sort(self, tables: List[str], dependencies: Dict[str, Set[str]]) -> List[str]:
        """
        Perform topological sort on tables based on their dependencies.
        
        Args:
            tables: List of table names
            dependencies: Dictionary mapping table names to sets of tables they depend on
            
        Returns:
            List of table names in topologically sorted order
        """
        # Track visited and temporary marks for cycle detection
        permanent_marks = set()
        temporary_marks = set()
        result = []
        
        def visit(table):
            """Recursive function to visit nodes in the dependency graph"""
            if table in permanent_marks:
                return
            if table in temporary_marks:
                # Circular dependency detected
                self.logger.warning(f"Circular dependency detected involving table: {table}")
                return
            
            temporary_marks.add(table)
            
            # Visit dependencies first
            for dependency in dependencies.get(table, set()):
                visit(dependency)
            
            temporary_marks.remove(table)
            permanent_marks.add(table)
            result.append(table)
        
        # Visit each table
        for table in tables:
            if table not in permanent_marks:
                visit(table)
        
        # Reverse the result to get correct order (dependencies first)
        return list(reversed(result))
    
    def log_migration_order(self, table_order: List[str]) -> None:
        """
        Log the migration order to maria_config.ini in the format:
        [migration]
        table1
        table2
        table3
        """
        # Read the existing config file
        config = configparser.ConfigParser(allow_no_value=True)
        if os.path.exists('maria_config.ini'):
            config.read('maria_config.ini')
        
        # Ensure the migration section exists
        if not config.has_section('migration'):
            config.add_section('migration')
        else:
            # Clear existing entries in the migration section
            config.remove_section('migration')
            config.add_section('migration')
        
        # Add each table on a new line
        for table in table_order:
            config.set('migration', table)
        
        # Write the updated config back to the file
        with open('maria_config.ini', 'w') as configfile:
            config.write(configfile)
        
        self.logger.info(f"Migration order saved to maria_config.ini")