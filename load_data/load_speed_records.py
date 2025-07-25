
"""
Load speed data from parquet file into PostgreSQL table.

This script loads data from duval_jan1_2024.parquet.gz into a new PostgreSQL table
'speed_record' in the 'urbansdk' database, keeping only the specified columns:
- link_id
- date_time  
- average_speed
- day_of_week
- period
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os
import sys
import gzip
import shutil
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'urbansdk'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

# Load configuration from environment variables
SPEED_RECORD_FILE = os.getenv('SPEED_RECORD_FILE', r"C:\Learn\urbansdk\duval_jan1_2024.parquet")
SPEED_TABLE = os.getenv('SPEED_TABLE', 'speed_record')
REQUIRED_COLUMNS_STR = os.getenv('REQUIRED_COLUMNS', 'link_id,date_time,average_speed,day_of_week,period')
REQUIRED_COLUMNS = [col.strip() for col in REQUIRED_COLUMNS_STR.split(',')]

def create_database_engine():
    """Create SQLAlchemy engine for database connection."""
    # Check if password is provided
    if not DATABASE_CONFIG['password']:
        print("Error: DB_PASSWORD not found in .env file")
        print("Please create a .env file in the project root with:")
        print("DB_USER=your_username")
        print("DB_PASSWORD=your_password")
        print("DB_HOST=localhost")
        print("DB_PORT=5432")
        print("DB_NAME=urbansdk")
        sys.exit(1)
    
    connection_string = (
        f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}"
        f"@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}"
        f"/{DATABASE_CONFIG['database']}"
    )
    return create_engine(connection_string)

def check_file_exists(file_path):
    """Check if the parquet file exists."""
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    print(f"Found file: {file_path}")

def is_gzipped_file(file_path):
    """Check if file is actually gzipped by reading magic bytes."""
    try:
        with open(file_path, 'rb') as f:
            # Read first 2 bytes to check for gzip magic number
            magic = f.read(2)
            return magic == b'\x1f\x8b'  # Gzip magic bytes
    except Exception:
        return False

def decompress_if_gzipped(file_path):
    """Decompress file if it's actually gzipped (checked by magic bytes, not just extension)."""
    
    # Check if file is actually gzipped
    if is_gzipped_file(file_path):
        print(f"File is gzipped (detected by magic bytes), decompressing...")
        
        # Create output filename by removing .gz extension if present, or adding .decompressed
        if file_path.endswith('.gz'):
            output_path = file_path[:-3]  # Remove .gz
        else:
            output_path = file_path + '.decompressed'
        
        # Check if decompressed file already exists
        if os.path.exists(output_path):
            print(f"Decompressed file already exists: {output_path}")
            response = input("Do you want to (u)se existing, (r)ecompress, or (c)ancel? [u/r/c]: ").lower().strip()
            
            if response == 'u':
                print("Using existing decompressed file")
                return output_path
            elif response == 'r':
                print("Removing existing file and decompressing...")
                os.remove(output_path)
            elif response == 'c':
                print("Operation cancelled")
                sys.exit(0)
            else:
                print("Invalid choice. Using existing file.")
                return output_path
        
        try:
            # Decompress the file
            with gzip.open(file_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            print(f"Successfully decompressed to: {output_path}")
            
            # Check file sizes
            original_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            decompressed_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            print(f"Original size: {original_size:.1f} MB")
            print(f"Decompressed size: {decompressed_size:.1f} MB")
            
            return output_path
            
        except Exception as e:
            print(f"Error decompressing file: {str(e)}")
            sys.exit(1)
    else:
        if file_path.endswith('.gz'):
            print(f"File has .gz extension but is not actually gzipped (magic bytes check failed)")
            print(f"Proceeding with original file - it may be a misnamed parquet file")
        else:
            print("File is not gzipped, proceeding with original file")
        return file_path

def load_parquet_data(file_path):
    """Load data from parquet file and select required columns."""
    print("Loading parquet file...")
    try:
        # Load the parquet file
        df = pd.read_parquet(file_path)
        print(f"Loaded {len(df)} rows from parquet file")
        print(f"Available columns: {list(df.columns)}")
        
        # Check if all required columns exist
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            sys.exit(1)
        
        # Select only the required columns
        df_filtered = df[REQUIRED_COLUMNS].copy()
        print(f"Selected columns: {REQUIRED_COLUMNS}")
        print(f"Filtered dataset shape: {df_filtered.shape}")
        
        # Display data types and sample data
        print("\nData types:")
        print(df_filtered.dtypes)
        print("\nSample data (first 5 rows):")
        print(df_filtered.head())
        
        return df_filtered
        
    except Exception as e:
        print(f"Error loading parquet file: {str(e)}")
        sys.exit(1)

def create_table_if_not_exists(engine):
    """Create the speed_record table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SPEED_TABLE} (
        id SERIAL PRIMARY KEY,
        link_id INTEGER NOT NULL,
        date_time TIMESTAMP NOT NULL,
        average_speed DECIMAL(10, 2),
        day_of_week INTEGER,
        period INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes for better query performance
    CREATE INDEX IF NOT EXISTS idx_{SPEED_TABLE}_link_id ON {SPEED_TABLE}(link_id);
    CREATE INDEX IF NOT EXISTS idx_{SPEED_TABLE}_date_time ON {SPEED_TABLE}(date_time);
    CREATE INDEX IF NOT EXISTS idx_{SPEED_TABLE}_day_period ON {SPEED_TABLE}(day_of_week, period);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print(f"Table '{SPEED_TABLE}' created or already exists")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        sys.exit(1)

def check_existing_data(engine):
    """Check if table already has data and ask user for confirmation."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {SPEED_TABLE}"))
            count = result.scalar()
            
        if count > 0:
            print(f"Warning: Table '{SPEED_TABLE}' already contains {count} rows")
            response = input("Do you want to (a)ppend, (r)eplace, or (c)ancel? [a/r/c]: ").lower().strip()
            
            if response == 'r':
                print("Truncating existing data...")
                with engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {SPEED_TABLE}"))
                    conn.commit()
                print("Existing data removed")
            elif response == 'c':
                print("Operation cancelled")
                sys.exit(0)
            elif response == 'a':
                print("Will append new data to existing records")
            else:
                print("Invalid choice. Operation cancelled")
                sys.exit(0)
        else:
            print(f"Table '{SPEED_TABLE}' is empty, ready for data loading")
            
    except Exception as e:
        print(f"Error checking existing data: {str(e)}")
        sys.exit(1)

def load_data_to_database(df, engine):
    """Load DataFrame to PostgreSQL table."""
    print("Loading data to database...")
    try:
        # Load data in chunks to handle large datasets
        chunk_size = 10000
        total_rows = len(df)
        
        for i in range(0, total_rows, chunk_size):
            chunk = df[i:i + chunk_size]
            chunk.to_sql(
                SPEED_TABLE, 
                engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            
            progress = min(i + chunk_size, total_rows)
            print(f"Loaded {progress}/{total_rows} rows ({progress/total_rows*100:.1f}%)")
        
        print(f"Successfully loaded {total_rows} rows into table '{SPEED_TABLE}'")
        
    except Exception as e:
        print(f"Error loading data to database: {str(e)}")
        sys.exit(1)

def verify_data_load(engine):
    """Verify the data was loaded correctly."""
    try:
        with engine.connect() as conn:
            # Get row count
            result = conn.execute(text(f"SELECT COUNT(*) FROM {SPEED_TABLE}"))
            count = result.scalar()
            
            # Get sample data
            sample_result = conn.execute(text(f"SELECT * FROM {SPEED_TABLE} LIMIT 5"))
            sample_data = sample_result.fetchall()
            
        print(f"\nVerification:")
        print(f"Total rows in table: {count}")
        print(f"Sample data from table:")
        for row in sample_data:
            print(f"  {dict(row._mapping)}")
            
    except Exception as e:
        print(f"Error verifying data: {str(e)}")

def main():
    """Main function to orchestrate the data loading process."""
    print("=== Speed Data Loader ===")
    print(f"Loading data from: {SPEED_RECORD_FILE}")
    print(f"Target table: {DATABASE_CONFIG['database']}.{SPEED_TABLE}")
    print(f"Required columns: {REQUIRED_COLUMNS}")
    print()
    
    # Check if parquet file exists
    check_file_exists(SPEED_RECORD_FILE)
    
    # Decompress file if it's gzipped
    actual_file_path = decompress_if_gzipped(SPEED_RECORD_FILE)
    
    # Create database engine
    print("Connecting to database...")
    engine = create_database_engine()
    
    # Test database connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection successful")
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        sys.exit(1)
    
    # Create table if it doesn't exist
    create_table_if_not_exists(engine)
    
    # Check for existing data
    check_existing_data(engine)
    
    # Load parquet data
    df = load_parquet_data(actual_file_path)
    
    # Load data to database
    load_data_to_database(df, engine)
    
    # Verify the load
    verify_data_load(engine)
    
    print("\n=== Data loading completed successfully! ===")

if __name__ == "__main__":
    main()
