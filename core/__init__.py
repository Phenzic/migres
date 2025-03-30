import os
import pymysql
import pandas as pd
import configparser
import subprocess
import psycopg2
import argparse
import uuid
from psycopg2.extras import execute_values
import numpy as np
import csv
import time

# Add timer start at the beginning of the script
start_time = time.time()

# Add timer start at the beginning of the script
start_time = time.time()

# Parse command line arguments
parser = argparse.ArgumentParser(description='Extract and migrate data from MariaDB to Supabase')
parser.add_argument('--no-download', action='store_true', help='Process and migrate data without saving files locally')
args = parser.parse_args()

# Load database and export configurations
config = configparser.ConfigParser()
config.read("./doujins_config.ini")

# Load type conversion configurations
type_config = configparser.ConfigParser()
type_config.read("./type_config.ini")

DB_CONFIG = {
    "host": config["mariadb"]["host"],
    "user": config["mariadb"]["user"],
    "password": config["mariadb"]["password"],
    "database": config["mariadb"]["database1"],




    # Add timeout and reconnect settings
    "connect_timeout": 28800,
    "read_timeout": 28800,
    "write_timeout": 28800,
    "charset": "utf8mb4",
}

PG_CONNECTION_STRING = config["supabase"]["PG_CONNECTION_STRING"]
# PG_CONNECTION_STRING = config["supabase"]["PG_CONNECTION_STRING_2"]

TABLES_TO_EXPORT = {
    config["mariadb"]["database3"]:["tags", "categories", "videos", "video_comments", "posts","video_tag", "video_user", "favorites"],
}


COLUMNS_TO_EXPORT = {
    
    "tags": ["id", "name", "total_used", "total_used_free", "total_used_censor"],
    "categories": ["id", "name", "video_id", "sort"],
    "posts": ["id", "user_id", "name", "content", "total_views", "created_at", "updated_at"],
    "video_comments": ["id", "video_id", "user_id", "name", "comment", "created_at", "updated_at", "quote_comment", "quote_name", "likes", "likes_users", "post_id", "comment_type", "edited"],
    "video_tag": ["id", "video_id", "tag_id"],
    "video_user": ["id", "video_id", "user_id", "created_at"],
    "videos": ["id", "access", "name", "episode", "episode_name", "alias", "censor", "hmeter", "description", "based", "duration", "poster", "master", "created_at", "updated_at", "path", "src", "views", "converted", "ratings", "total_comments"],
    "favorites": [ "id", "user_id", "video_id" ,"created_at", "updated_at"]
    
}

EXPORT_DIR = "./target_folder"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Load or create uuid_config.ini
uuid_config_path = "./uuid_config.ini"
uuid_config = configparser.ConfigParser()
uuid_config.read(uuid_config_path)

# Load table schema configurations
schema_config = configparser.ConfigParser()
schema_config.read("./table_schema.ini")

# Connect to MariaDB
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

# Define table and column name mappings
TABLE_NAME_MAPPING = {
    "video_user": "hentai.video_user",
    "posts": "hentai.posts",
    "tags": "hentai.tags",
    "categories": "hentai.categories",
    "videos": "hentai.videos",
    "video_comments": "hentai.video_comments",
    "video_tag": "hentai.video_tag",
    "favorites": "hentai.favorites"
}

COLUMN_NAME_MAPPING = {
}


# Modify the upload_to_postgres function to handle schema properly
def upload_to_postgres(table_name, data):
    max_retries = 3
    retry_delay = 5  # seconds
    batch_size = 1000000  # Reduced batch size for better reliability
    
    for attempt in range(max_retries):
        try:
            pg_conn = psycopg2.connect(PG_CONNECTION_STRING)
            pg_cursor = pg_conn.cursor()

            # Get the mapped table name with schema
            mapped_table = TABLE_NAME_MAPPING.get(table_name, table_name)
            
            # Convert data to list of tuples and apply column mappings
            column_names = data[0].keys()
            mapped_column_names = [COLUMN_NAME_MAPPING.get(table_name, {}).get(col, col) for col in column_names]
            
            # Process in smaller batches
            total_rows = len(data)
            
            for i in range(0, total_rows, batch_size):
                try:
                    if pg_conn.closed:
                        pg_conn = psycopg2.connect(PG_CONNECTION_STRING)
                        pg_cursor = pg_conn.cursor()
                    
                    batch = data[i:i + batch_size]
                    values = [[row[col] for col in column_names] for row in batch]
                    
                    columns_str = ', '.join(f'"{col}"' for col in mapped_column_names)
                    query = f'INSERT INTO {mapped_table} ({columns_str}) VALUES %s'
                    
                    execute_values(pg_cursor, query, values)
                    pg_conn.commit()
                    print(f"Successfully uploaded batch {i//batch_size + 1} ({len(batch)} records) to {mapped_table}")
                
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    print(f"Connection error during batch upload: {str(e)}")
                    if not pg_conn.closed:
                        pg_conn.close()
                    if attempt < max_retries - 1:
                        print(f"Retrying batch in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise

            print(f"Completed uploading all {total_rows} records to {mapped_table}")
            return True

        except Exception as e:
            print(f"Error uploading {table_name}: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying entire upload in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            else:
                if not pg_conn.closed:
                    pg_conn.rollback()
                return False

        finally:
            if 'pg_cursor' in locals() and pg_cursor:
                pg_cursor.close()
            if 'pg_conn' in locals() and not pg_conn.closed:
                pg_conn.close()

def get_schema_for_table(table):
    """Get the schema for a given table from schema configuration"""
    try:
        # For tables from database3, always use hentai schema
        if table in TABLES_TO_EXPORT.get(config["mariadb"]["database3"], []):
            return 'hentai'
        # Check schema config for other tables
        if 'schemas' in schema_config and table in schema_config['schemas']:
            return schema_config['schemas'][table]
        return 'public'
    except Exception as e:
        print(f"Error getting schema for table {table}: {str(e)}")
        return 'public'

def get_target_table_name(db_name, table):
    """Get the target table name with appropriate prefix based on database"""
    if db_name == config["mariadb"]["database3"]:
        return table  # Don't add prefix, we'll handle it in TABLE_NAME_MAPPING
    return table

# Function to create tables in PostgreSQL
def create_tables_in_postgres():
    """Create all necessary schemas and tables in PostgreSQL"""
    try:
        pg_conn = psycopg2.connect(PG_CONNECTION_STRING)
        pg_cursor = pg_conn.cursor()

        # First create schemas
        pg_cursor.execute('CREATE SCHEMA IF NOT EXISTS auth;')
        pg_cursor.execute('CREATE SCHEMA IF NOT EXISTS doujins;')
        pg_cursor.execute('CREATE SCHEMA IF NOT EXISTS hentai;')
        pg_conn.commit()
        print("Created schemas successfully")

        # Create tables for each table in TABLES_TO_EXPORT
        for db_name, tables in TABLES_TO_EXPORT.items():
            for table in tables:
                try:
                    # Get the target table name (with hentai_ prefix if needed)
                    target_table = get_target_table_name(db_name, table)
                    
                    # Get the schema for this table
                    schema = get_schema_for_table(target_table)  # Use target_table instead of table
                    
                    # Get column definitions from schema_config
                    if target_table in schema_config.sections():
                        columns = []
                        for column, data_type in schema_config.items(target_table):
                            if column != 'schema':  # Skip schema entry if it exists
                                if column == 'id':
                                    columns.append(f'"{column}" {data_type} PRIMARY KEY')
                                else:
                                    columns.append(f'"{column}" {data_type}')
                        
                        if columns:  # Only create table if we have columns
                            columns_str = ', '.join(columns)
                            create_table_query = f'''
                                CREATE TABLE IF NOT EXISTS {schema}."{target_table}" (
                                    {columns_str}
                                );
                            '''
                            print(f"Creating table {schema}.{target_table}")
                            pg_cursor.execute(create_table_query)
                            pg_conn.commit()
                            print(f"Created table {schema}.{target_table}")
                        else:
                            print(f"Warning: No columns defined for table {target_table}")
                    else:
                        print(f"Warning: No schema definition found for table {target_table}")
                
                except Exception as e:
                    print(f"Error creating table {table}: {str(e)}")
                    print(f"Failed query for debugging: {create_table_query if 'create_table_query' in locals() else 'No query generated'}")
                    continue

        print("All tables created successfully")
        
    except Exception as e:
        print(f"Error in create_tables_in_postgres: {str(e)}")
        if 'pg_conn' in locals():
            pg_conn.rollback()
    finally:
        if 'pg_cursor' in locals():
            pg_cursor.close()
        if 'pg_conn' in locals():
            pg_conn.close()

# Make sure to call this function before processing any data
create_tables_in_postgres()

def convert_types(df, table_name):
    """Convert column types based on type_config.ini specifications"""
    if table_name not in type_config.sections():
        return df
    
    df_copy = df.copy()
    for column, type_conversion in type_config.items(table_name):
        if column not in df_copy.columns:
            continue
            
        print(f"Converting column: {column} to {type_conversion}")  # Debug
        try:
            if type_conversion.lower() == 'boolean':
                df_copy[column] = df_copy[column].astype(int).map({1: True, 0: False})
            elif type_conversion.lower() == 'timestamp':
                df_copy[column] = pd.to_datetime(df_copy[column], unit='s')
            elif type_conversion.lower() == 'int':
                # Ensure integers are preserved as strings to prevent float conversion
                df_copy[column] = df_copy[column].astype(int).astype(str)
            elif type_conversion.lower() == 'float':
                df_copy[column] = df_copy[column].astype("float64")
        except Exception as e:
            print(f"Error converting column {column}: {str(e)}")  # Debug
            raise e
            
    return df_copy

def generate_uuid_for_column(df, column_name, table_name, uuid_config):
    """Generate deterministic UUIDs using UUID5 with prefix names"""
    if table_name not in uuid_config:
        print(f"Warning: No UUID configuration found for table {table_name}")
        return df
    
    # Get the prefix name for this column from config
    prefix = uuid_config[table_name].get(column_name)
    if not prefix:
        print(f"Warning: No prefix configured for column {column_name} in table {table_name}")
        return df
    
    # Create a namespace UUID for this prefix
    namespace = uuid.NAMESPACE_DNS
    
    def generate_uuid5(value):
        if pd.isna(value):
            return None
        # Create a deterministic string combining prefix and ID
        name = f"{prefix}_{value}"
        return str(uuid.uuid5(namespace, name))
    
    df[column_name] = df[column_name].apply(generate_uuid5)
    return df

def convert_uuids(df, table_name):
    """Convert all UUID columns for a table using UUID5"""
    if table_name not in uuid_config.sections():
        return df
    
    df_copy = df.copy()
    # Get all columns that need UUID conversion from uuid_config
    uuid_columns = [col for col in uuid_config[table_name].keys()]
    
    for column in uuid_columns:
        if column in df_copy.columns:
            df_copy = generate_uuid_for_column(df_copy, column, table_name, uuid_config)
    
    return df_copy

def clean_data(df, table_name):
    # Convert all pandas NaT to None
    df = df.replace({pd.NaT: None})
    
    # Additional cleaning for timestamp columns
    timestamp_columns = []
    if table_name in schema_config.sections():
        for column, data_type in schema_config.items(table_name):
            if 'timestamp' in data_type.lower():
                timestamp_columns.append(column)
    
    for col in timestamp_columns:
        if col in df.columns:
            # Convert any remaining datetime objects or 'NaT' strings to None
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or str(x) == 'NaT' else x)
    
    return df

def wait_for_export(file_path, timeout=300):  # 5 minutes timeout
    """Wait for file to exist and be non-empty"""
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Export timed out waiting for {file_path}")
            
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Read first line to check if file has content
                    first_line = f.readline()
                    if first_line.strip():
                        # File exists and has content
                        print(f"Export complete: {file_path}")
                        # Wait an extra second to ensure file is fully written
                        time.sleep(1)
                        return True
            except Exception as e:
                print(f"Error checking file {file_path}: {e}")
                
        print(f"Waiting for export to complete: {file_path}")
        time.sleep(5)  # Check every 5 seconds

def ensure_uuid_config(table_name, uuid_config):
    """Ensure UUID configuration exists for the given table"""
    if table_name not in uuid_config.sections():
        print(f"Creating new UUID configuration for {table_name}")
        uuid_config.add_section(table_name)
        # Set default prefix as table name without trailing 's'
        base_prefix = table_name.rstrip('s')
        uuid_config.set(table_name, 'id', base_prefix)
        
        # Save the updated config
        with open(uuid_config_path, 'w') as configfile:
            uuid_config.write(configfile)
        print(f"Created default UUID configuration for table {table_name}")
    
    return uuid_config

def process_and_upload_table(table, df, uuid_config):
    """Process a single table's data and upload to PostgreSQL"""
    if len(df) == 0:
        print(f"Warning: No data for table {table}. Skipping.")
        return

    try:
        print(f"Processing {table} in memory...")
        
        # First convert UUIDs
        print(f"Converting UUIDs for {table}...")
        df_uuid_converted = convert_uuids(df, table)
        
        # Then apply type conversions
        print(f"Converting column types for {table}...")
        df_converted = convert_types(df_uuid_converted, table)
        
        # Clean the data before insertion
        df_cleaned = clean_data(df_converted, table)
        
        # Convert to dict and upload to PostgreSQL
        print(f"Uploading {table} to PostgreSQL...")
        data_converted = df_cleaned.to_dict(orient="records")
        upload_to_postgres(table, data_converted)
        print(f"Successfully processed and uploaded {table}")
    except Exception as e:
        print(f"Error processing {table}: {str(e)}")

def get_mysql_connection():
    """Create a new MySQL connection with retry logic"""
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            connection = pymysql.connect(**DB_CONFIG)
            # Set session variables for longer timeouts
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION wait_timeout=28800")
                cursor.execute("SET SESSION interactive_timeout=28800")
                cursor.execute("SET SESSION net_read_timeout=7200")
                cursor.execute("SET SESSION net_write_timeout=7200")
            return connection
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise e

def read_table_in_chunks(connection, table, columns, chunk_size=500000):
    """Read a table in chunks with connection retry logic"""
    chunks = []
    offset = 0
    max_retries = 3
    
    while True:
        for attempt in range(max_retries):
            try:
                # Create a new connection for each chunk if the current one is closed
                if not connection.open:
                    connection = get_mysql_connection()

                # Create the chunked query without database prefix
                column_str = ", ".join(f"`{col}`" for col in columns)
                query = f"SELECT {column_str} FROM `{table}` LIMIT {chunk_size} OFFSET {offset}"
                print(f"Executing query: {query}")  # Debug query
                
                # Read the chunk
                chunk = pd.read_sql(query, connection)
                
                if chunk.empty:
                    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
                    
                chunks.append(chunk)
                print(f"Read chunk of {len(chunk)} rows from offset {offset}")
                offset += chunk_size
                
                # If the chunk is smaller than chunk_size, we've reached the end
                if len(chunk) < chunk_size:
                    return pd.concat(chunks, ignore_index=True)
                    
                break  # Success, break retry loop
                
            except pymysql.err.OperationalError as e:
                print(f"MySQL Operational Error: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"Connection error on attempt {attempt + 1}: {str(e)}. Retrying...")
                    time.sleep(5)  # Wait before retry
                    connection = get_mysql_connection()
                else:
                    print(f"Failed to read chunk after {max_retries} attempts")
                    raise
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                raise

# Modify the main processing loop
for db_name, tables in TABLES_TO_EXPORT.items():
    print(f"\nProcessing database: {db_name}")
    
    try:
        # Create a fresh connection and select the database
        conn = get_mysql_connection()
        conn.select_db(db_name)
        print(f"Successfully connected to database: {db_name}")
        
        for table in tables:
            print(f"Exporting {table} (all rows)...")
            columns = COLUMNS_TO_EXPORT.get(table, ["*"])
            
            try:
                # Read the table in chunks with retry logic
                print(f"Reading data from {table}...")
                df = read_table_in_chunks(conn, table, columns)
                print(f"Total rows read from {table}: {len(df)}")
                
                if not df.empty:
                    process_and_upload_table(table, df, uuid_config)
                else:
                    print(f"Warning: No data found in table {table}")
                    
            except Exception as e:
                print(f"Error processing table {table}: {str(e)}")
                continue
            
            time.sleep(1)  # Small delay between tables
            
    except Exception as e:
        print(f"Error processing database {db_name}: {str(e)}")
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

# Special handling for images table
print("\nProcessing images table from objects data...")

# Add at the end of the script, after all processing is complete
end_time = time.time()
total_time = end_time - start_time
hours = int(total_time // 3600)
minutes = int((total_time % 3600) // 60)
seconds = int(total_time % 60)

print(f"\nTotal migration time: {hours} hours, {minutes} minutes, {seconds} seconds")

def get_postgres_connection():
    """Create a new PostgreSQL connection with retry logic"""
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            connection = psycopg2.connect(PG_CONNECTION_STRING)
            return connection
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise e

def apply_constraints():
    """Apply all constraints after data migration"""
    config = configparser.ConfigParser()
    config.read('./constraints.ini')
    
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        
        for table in config.sections():
            schema = get_schema_for_table(table)
            print(f"\nApplying constraints for {schema}.{table}")
            
            # Primary Key
            if config.has_option(table, 'primary_key'):
                pk = config.get(table, 'primary_key')
                cur.execute(f"""
                    ALTER TABLE {schema}.{table}
                    ADD CONSTRAINT {table}_pkey PRIMARY KEY ({pk});
                """)
                print(f"Added primary key on {pk}")
            
            # Unique Constraints
            if config.has_option(table, 'unique'):
                unique_cols = [col.strip() for col in config.get(table, 'unique').split(',')]
                for i, cols in enumerate(unique_cols):
                    constraint_name = f"{table}_unique_{i}"
                    cur.execute(f"""
                        ALTER TABLE {schema}.{table}
                        ADD CONSTRAINT {constraint_name} UNIQUE ({cols});
                    """)
                    print(f"Added unique constraint on {cols}")
            
            # Foreign Keys
            if config.has_option(table, 'foreign_keys'):
                fk_definitions = [fk.strip() for fk in config.get(table, 'foreign_keys').split('\n') if fk.strip()]
                for fk in fk_definitions:
                    local_col, reference = fk.split('->')
                    local_col = local_col.strip()
                    reference = reference.strip()
                    constraint_name = f"{table}_{local_col}_fkey"
                    
                    cur.execute(f"""
                        ALTER TABLE {schema}.{table}
                        ADD CONSTRAINT {constraint_name}
                        FOREIGN KEY ({local_col})
                        REFERENCES {reference}
                        ON DELETE CASCADE;
                    """)
                    print(f"Added foreign key from {local_col} to {reference}")
            
            # Indexes
            if config.has_option(table, 'indexes'):
                index_cols = [col.strip() for col in config.get(table, 'indexes').split(',')]
                for col in index_cols:
                    index_name = f"{table}_{col}_idx"
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {schema}.{table} ({col});
                    """)
                    print(f"Created index on {col}")
        
        conn.commit()
        print("\nSuccessfully applied all constraints")
        
    except Exception as e:
        print(f"Error applying constraints: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# Add this to the end of your main script
if __name__ == "__main__":
    # ... existing migration code ...
    
    print("\nApplying constraints...")
    apply_constraints()