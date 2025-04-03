import os


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
        for column in excluded_columns:
            if column:
                content += f"{column.strip()}\n"
    
    with open("maria_config.ini", "w") as f:
        f.write(content)
    
    print(f"Successfully appended excluded columns: {', '.join(excluded_columns)}")
    
    return 0