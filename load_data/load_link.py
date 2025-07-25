"""
Load link data from parquet file into PostgreSQL table with PostGIS geometry support.

This script loads link data into a PostgreSQL table with PostGIS extension,
keeping only the specified columns:
- link_id
- road_name
- geo_json (converted to geometry(LINESTRING) type)
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
import json

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
LINK_FILE = os.getenv('LINK_FILE', r"C:\Learn\urbansdk\links.parquet")
LINK_TABLE = os.getenv('LINK_TABLE', 'link')
REQUIRED_COLUMNS_STR = os.getenv('LINK_COLUMNS', 'link_id,road_name,geo_json')
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

def check_postgis_extension(engine):
    """Check if PostGIS extension is enabled."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis')"))
            has_postgis = result.scalar()
            
        if not has_postgis:
            print("Warning: PostGIS extension is not enabled in this database.")
            print("Attempting to enable PostGIS extension...")
            try:
                with engine.connect() as conn:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                    conn.commit()
                print("PostGIS extension enabled successfully")
            except Exception as e:
                print(f"Error enabling PostGIS extension: {str(e)}")
                print("Please ensure you have PostGIS installed and proper permissions")
                sys.exit(1)
        else:
            print("PostGIS extension is available")
            
    except Exception as e:
        print(f"Error checking PostGIS extension: {str(e)}")
        sys.exit(1)

def create_table_if_not_exists(engine):
    """Create the link table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {LINK_TABLE} (
        id SERIAL PRIMARY KEY,
        link_id INTEGER NOT NULL UNIQUE,
        road_name VARCHAR(255),
        geometry geometry(GEOMETRY, 4326),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes for better query performance
    CREATE INDEX IF NOT EXISTS idx_{LINK_TABLE}_link_id ON {LINK_TABLE}(link_id);
    CREATE INDEX IF NOT EXISTS idx_{LINK_TABLE}_road_name ON {LINK_TABLE}(road_name);
    CREATE INDEX IF NOT EXISTS idx_{LINK_TABLE}_geometry ON {LINK_TABLE} USING GIST(geometry);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print(f"Table '{LINK_TABLE}' created or already exists")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        sys.exit(1)

def check_existing_data(engine):
    """Check if table already has data and ask user for confirmation."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {LINK_TABLE}"))
            count = result.scalar()
            
        if count > 0:
            print(f"Warning: Table '{LINK_TABLE}' already contains {count} rows")
            response = input("Do you want to (a)ppend, (r)eplace, or (c)ancel? [a/r/c]: ").lower().strip()
            
            if response == 'r':
                print("Truncating existing data...")
                with engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {LINK_TABLE}"))
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
            print(f"Table '{LINK_TABLE}' is empty, ready for data loading")
            
    except Exception as e:
        print(f"Error checking existing data: {str(e)}")
        sys.exit(1)

def validate_geojson(geo_json_str):
    """Validate and convert GeoJSON string to WKT format for PostGIS."""
    try:
        if pd.isna(geo_json_str) or geo_json_str is None:
            return None
            
        # Convert to string if not already
        geo_str = str(geo_json_str)
        
        # If it's already a string that looks like WKT, return it
        if geo_str.upper().startswith(('LINESTRING', 'MULTILINESTRING')):
            return geo_str
            
        # Try to parse as JSON
        try:
            geom_data = json.loads(geo_str)
        except json.JSONDecodeError as e:
            print(f"Warning: Invalid JSON geometry data: {geo_str[:100]}... Error: {str(e)}")
            return None
            
        # Handle GeoJSON geometry format
        if isinstance(geom_data, dict):
            geom_type = geom_data.get('type')
            coords = geom_data.get('coordinates')
            
            if geom_type == 'LineString':
                # Convert LineString coordinates to WKT format
                if not coords or len(coords) < 2:
                    print(f"Warning: Invalid LineString coordinates: {coords}")
                    return None
                    
                try:
                    coord_pairs = []
                    for coord in coords:
                        if len(coord) >= 2:
                            coord_pairs.append(f"{float(coord[0])} {float(coord[1])}")
                        else:
                            print(f"Warning: Invalid coordinate pair: {coord}")
                            return None
                    
                    wkt = f"LINESTRING({', '.join(coord_pairs)})"
                    return wkt
                except (ValueError, TypeError) as e:
                    print(f"Warning: Error converting LineString coordinates to WKT: {str(e)}")
                    return None
            
            elif geom_type == 'MultiLineString':
                # Convert MultiLineString coordinates to WKT format
                if not coords or len(coords) == 0:
                    print(f"Warning: Invalid MultiLineString coordinates: {coords}")
                    return None
                    
                try:
                    linestring_parts = []
                    for linestring_coords in coords:
                        if len(linestring_coords) < 2:
                            print(f"Warning: LineString in MultiLineString has < 2 points: {linestring_coords}")
                            continue
                            
                        coord_pairs = []
                        for coord in linestring_coords:
                            if len(coord) >= 2:
                                coord_pairs.append(f"{float(coord[0])} {float(coord[1])}")
                            else:
                                print(f"Warning: Invalid coordinate pair: {coord}")
                                break
                        else:
                            # Only add if all coordinates were valid (no break)
                            linestring_parts.append(f"({', '.join(coord_pairs)})")
                    
                    if not linestring_parts:
                        print("Warning: No valid LineStrings found in MultiLineString")
                        return None
                    
                    # If there's only one LineString, convert to simple LINESTRING
                    if len(linestring_parts) == 1:
                        wkt = f"LINESTRING{linestring_parts[0]}"
                    else:
                        wkt = f"MULTILINESTRING({', '.join(linestring_parts)})"
                    
                    return wkt
                except (ValueError, TypeError) as e:
                    print(f"Warning: Error converting MultiLineString coordinates to WKT: {str(e)}")
                    return None
            
            elif geom_type:
                print(f"Warning: Unsupported geometry type: {geom_type}")
                return None
            else:
                print(f"Warning: Invalid GeoJSON structure: {geo_str[:100]}...")
                return None
        else:
            print(f"Warning: Geometry data is not a dictionary: {type(geom_data)}")
            return None
        
    except Exception as e:
        print(f"Warning: Could not process geometry: {str(e)} - Data: {str(geo_json_str)[:100]}...")
        return None

def prepare_data_for_loading(df):
    """Prepare DataFrame for loading into PostGIS table."""
    print("Preparing data for database loading...")
    
    # Create a copy of the dataframe
    df_prepared = df.copy()
    
    # Convert geo_json column to WKT format if needed
    if 'geo_json' in df_prepared.columns:
        print("Converting geo_json to WKT format...")
        df_prepared['geo_json'] = df_prepared['geo_json'].apply(validate_geojson)
        
        # Count null geometries
        null_geom_count = df_prepared['geo_json'].isna().sum()
        if null_geom_count > 0:
            print(f"Warning: {null_geom_count} rows have null geometries and will be skipped")
            df_prepared = df_prepared.dropna(subset=['geo_json'])
    
    print(f"Prepared {len(df_prepared)} rows for loading")
    return df_prepared

def load_data_to_database(df, engine):
    """Load DataFrame to PostgreSQL table with geometry handling."""
    print("Loading data to database...")
    try:
        # Prepare data
        df_prepared = prepare_data_for_loading(df)
        
        if len(df_prepared) == 0:
            print("No valid data to load after preparation")
            return
        
        # Load data in chunks to handle large datasets
        chunk_size = 1000  # Smaller chunks for geometry data
        total_rows = len(df_prepared)
        
        for i in range(0, total_rows, chunk_size):
            chunk = df_prepared[i:i + chunk_size].copy()
            
            # Handle the geometry column specially
            for idx, row in chunk.iterrows():
                try:
                    with engine.connect() as conn:
                        # Use ST_GeomFromText to convert WKT to geometry
                        if 'geo_json' in row and row['geo_json'] is not None:
                            insert_sql = f"""
                            INSERT INTO {LINK_TABLE} (link_id, road_name, geometry) 
                            VALUES (:link_id, :road_name, ST_GeomFromText(:geometry, 4326))
                            ON CONFLICT (link_id) 
                            DO UPDATE SET 
                                road_name = EXCLUDED.road_name,
                                geometry = EXCLUDED.geometry
                            """
                            conn.execute(text(insert_sql), {
                                'link_id': row['link_id'],
                                'road_name': row.get('road_name', None),
                                'geometry': row['geo_json']
                            })
                        else:
                            # Insert without geometry if null
                            insert_sql = f"""
                            INSERT INTO {LINK_TABLE} (link_id, road_name) 
                            VALUES (:link_id, :road_name)
                            ON CONFLICT (link_id) 
                            DO UPDATE SET 
                                road_name = EXCLUDED.road_name
                            """
                            conn.execute(text(insert_sql), {
                                'link_id': row['link_id'],
                                'road_name': row.get('road_name', None)
                            })
                        conn.commit()
                except Exception as e:
                    print(f"Error inserting row {idx}: {str(e)}")
                    continue
            
            progress = min(i + chunk_size, total_rows)
            print(f"Loaded {progress}/{total_rows} rows ({progress/total_rows*100:.1f}%)")
        
        print(f"Successfully loaded {total_rows} rows into table '{LINK_TABLE}'")
        
    except Exception as e:
        print(f"Error loading data to database: {str(e)}")
        sys.exit(1)

def verify_data_load(engine):
    """Verify the data was loaded correctly."""
    try:
        with engine.connect() as conn:
            # Get row count
            result = conn.execute(text(f"SELECT COUNT(*) FROM {LINK_TABLE}"))
            count = result.scalar()
            
            # Get sample data with geometry info
            sample_result = conn.execute(text(f"""
                SELECT 
                    link_id, 
                    road_name, 
                    ST_AsText(geometry) as geometry_wkt,
                    ST_GeometryType(geometry) as geom_type
                FROM {LINK_TABLE} 
                WHERE geometry IS NOT NULL
                LIMIT 3
            """))
            sample_data = sample_result.fetchall()
            
            # Get count of rows with geometry
            geom_result = conn.execute(text(f"SELECT COUNT(*) FROM {LINK_TABLE} WHERE geometry IS NOT NULL"))
            geom_count = geom_result.scalar()
            
        print(f"\nVerification:")
        print(f"Total rows in table: {count}")
        print(f"Rows with geometry: {geom_count}")
        print(f"Sample data from table:")
        for row in sample_data:
            row_dict = dict(row._mapping)
            # Truncate geometry for display
            if row_dict.get('geometry_wkt'):
                geom_text = row_dict['geometry_wkt']
                if len(geom_text) > 100:
                    row_dict['geometry_wkt'] = geom_text[:100] + "..."
            print(f"  {row_dict}")
            
    except Exception as e:
        print(f"Error verifying data: {str(e)}")

def main():
    """Main function to orchestrate the data loading process."""
    print("=== Link Data Loader ===")
    print(f"Loading data from: {LINK_FILE}")
    print(f"Target table: {DATABASE_CONFIG['database']}.{LINK_TABLE}")
    print(f"Required columns: {REQUIRED_COLUMNS}")
    print()
    
    # Check if parquet file exists
    check_file_exists(LINK_FILE)
    
    # Decompress file if it's gzipped
    actual_file_path = decompress_if_gzipped(LINK_FILE)
    
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
    
    # Check PostGIS extension
    check_postgis_extension(engine)
    
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
    
    print("\n=== Link data loading completed successfully! ===")

if __name__ == "__main__":
    main()
