import pandas as pd
import requests
from io import StringIO, BytesIO
from typing import Iterator, Dict, Any
import logging   
import gzip
from sqlalchemy import create_engine, inspect
import os
import utils as utils
import pyarrow.parquet as pq
from one_time_load import zone_data_etl


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHUNK_SIZE = 10000

DTYPE_INT_COLS = [
    "VendorID",
    "passenger_count",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
]

DTYPE_FLOAT_COLS = [
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]

# Database configuration
DB_URI ='postgresql://pguser:pgpassword@localhost:5432/nyc_taxi'
engine = create_engine(DB_URI)


DB_CONFIG = {
    'host': 'localhost',
    'database': 'nyc_taxi',
    'user': 'pguser',
    'password': 'pgpassword',
    'port': 5432
}


def get_tripdata_url(zone: str = 'yellow', year: int = 2025, month: int = 11) -> str:
    """Constructs the URL for the trip data file based on color, year, and month.
    Args:
        zone (str): The zone of the trip data ('yellow' or 'green').
        year (int): The year of the trip data.
        month (int): The month of the trip data.
    Returns:
        str: The constructed URL for the trip data file.
    """

    
    # url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

    # if zone == 'yellow':
    #     prefix = 'yellow_tripdata_'
    # elif zone == 'green':
    #     prefix = 'green_tripdata_'
    # else:
    #     raise ValueError("zone must be either 'yellow' or 'green'.")

    # month_str = f"{month:02d}"  # Ensure month is two digits
    # file_name = f"{prefix}{year}-{month_str}.parquet"

    # return f"{url}{file_name}"


    ### Test-Run with csv.gz files
    if zone == 'yellow':
        yellow_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
        url = yellow_prefix + 'yellow_tripdata_2021-01.csv.gz'
    elif zone == 'green':
        green_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        url = green_prefix + 'green_tripdata_2025-11.parquet'
    else:
        raise ValueError("zone must be either 'yellow' or 'green'.")

    return url


def extract_parquet_chunks(url: str, chunk_size: int = CHUNK_SIZE) -> Iterator[pd.DataFrame]:
    """
    Extract Parquet data from URL in chunks.
    
    Args:
        url: URL of the Parquet file
        chunk_size: Number of rows per chunk
        
    Yields:
        DataFrame chunks
    """
    logger.info(f"Starting Parquet extraction from {url}")
    
    try:
        # Download the Parquet file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Load into BytesIO
        parquet_file = BytesIO(response.content)
        
        # Read Parquet file
        parquet_table = pq.read_table(parquet_file)
        
        # Get total number of rows
        total_rows = parquet_table.num_rows
        logger.info(f"Total rows in Parquet file: {total_rows}")
        
        # Process in chunks
        chunk_count = 0
        for i in range(0, total_rows, chunk_size):
            # Extract chunk
            end_idx = min(i + chunk_size, total_rows)
            chunk_table = parquet_table.slice(i, end_idx - i)
            
            # Convert to pandas DataFrame
            chunk = chunk_table.to_pandas()
            
            chunk_count += 1
            logger.info(f"Extracted chunk {chunk_count} with {len(chunk)} rows (rows {i} to {end_idx-1})")
            yield chunk
            
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Parquet from URL: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to process Parquet file: {e}")
        raise


def extract_csv_chunks(url: str, chunk_size: int = CHUNK_SIZE) -> Iterator[pd.DataFrame]:
    """
    Extract CSV data from URL in chunks.
    Handles both plain CSV and gzipped CSV files.
    
    Args:
        url: URL of the CSV file (can be .csv or .csv.gz)
        chunk_size: Number of rows per chunk
        
    Yields:
        DataFrame chunks
    """
    logger.info(f"Starting CSV extraction from {url}")
    
    try:
        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Check if file is gzipped based on URL or content
        is_gzipped = url.endswith('.gz')
        
        if is_gzipped:
            logger.info("Detected gzipped file, decompressing...")
            # Decompress gzipped content
            compressed_file = BytesIO(response.content)
            decompressed_file = gzip.GzipFile(fileobj=compressed_file)
            
            # Read CSV in chunks from decompressed data
            chunk_iterator = pd.read_csv(
                decompressed_file,
                chunksize=chunk_size,
                encoding='utf-8',
                on_bad_lines='skip',
            )
        else:
            # Read plain CSV
            csv_data = StringIO(response.text, newline='')
            chunk_iterator = pd.read_csv(
                csv_data,
                chunksize=chunk_size,
                encoding='utf-8',
                on_bad_lines='skip',
            )
        
        chunk_count = 0
        for chunk in chunk_iterator:
            chunk_count += 1
            logger.info(f"Extracted chunk {chunk_count} with {len(chunk)} rows")
            yield chunk
            
    except requests.RequestException as e:
        logger.error(f"Failed to fetch CSV from URL: {e}")
        raise
    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse CSV: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {e}")
        raise


def transform_data(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    """Transforms the input DataFrame by renaming columns and calculating trip duration.
    Args:
        df (pd.DataFrame): The input DataFrame containing trip data.
        zone (str): The zone of the taxi trip data ('yellow' or 'green').
    Returns:
        pd.DataFrame: The transformed DataFrame with renamed columns and trip duration.
    """
    logger.info(f"Transforming chunk with {len(df)} rows")

    df = normalize_schema(df, zone=zone)

    if zone=='yellow':
        df.rename(columns={
                'VendorID': 'vendor_id',
                'tpep_pickup_datetime': 'pickup_datetime',
                'tpep_dropoff_datetime': 'dropoff_datetime',
                'trip_distance': 'trip_distance_miles',
                'RatecodeID': 'rate_code_id',
                'store_and_fwd_flag': 'store_and_forward_flag',
                'PULocationID': 'pickup_location_id',
                'DOLocationID': 'dropoff_location_id'
            }, inplace=True)
    else:
        df.rename(columns={
                'VendorID': 'vendor_id',
                'lpep_pickup_datetime': 'pickup_datetime',
                'lpep_dropoff_datetime': 'dropoff_datetime',
                'trip_distance': 'trip_distance_miles',
                'RatecodeID': 'rate_code_id',
                'store_and_fwd_flag': 'store_and_forward_flag',
                'PULocationID': 'pickup_location_id',
                'DOLocationID': 'dropoff_location_id'
            }, inplace=True)

    df['trip_duration_secs'] = df[['dropoff_datetime','pickup_datetime']].apply(lambda x: (x['dropoff_datetime']-x['pickup_datetime']).seconds, axis=1)


    logger.info(f"Transformation complete - {len(df)} rows processed")

    return df


def normalize_schema(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    """Normalize data types and parse date fields after extraction."""
    
    if zone == "yellow":
        date_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    else:
        date_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in DTYPE_INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in DTYPE_FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string")

    return df


def etl_pipeline(url: str, zone: str, db_config: Dict[str, Any], table_name: str, chunk_size: int = CHUNK_SIZE, file_type: str = 'csv'):
    """
    Execute the complete ETL pipeline.
    
    Args:
        url: File URL (CSV, CSV.GZ, or Parquet)
        zone: The zone of the trip data ('yellow' or 'green')
        db_config: Database configuration
        table_name: Target table name
        chunk_size: Number of rows per chunk
        file_type: Type of file - 'csv', 'csv.gz', or 'parquet'
    """
    logger.info(f"Starting ETL pipeline for {file_type} file")
    
    total_rows = 0
    chunk_num = 0
    
    try:
        # Determine extraction method based on file type
        if file_type == 'parquet':
            chunk_iterator = extract_parquet_chunks(url, chunk_size)
        else:
            # Works for both 'csv' and 'csv.gz'
            chunk_iterator = extract_csv_chunks(url, chunk_size)
        
        # Process each chunk
        for chunk in chunk_iterator:
            chunk_num += 1
            
            # Transform
            transformed_chunk = transform_data(chunk, zone=zone)
            
            # Load
            utils.load_to_postgres(transformed_chunk, db_config, table_name)
            
            total_rows += len(transformed_chunk)
            logger.info(f"Processed chunk {chunk_num}. Total rows processed: {total_rows}")
        
        logger.info(f"ETL pipeline completed successfully. Total rows: {total_rows}")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


def main():

    files = [
        {'zone': 'yellow', 'year': 2021, 'month': 1, 'file_type': 'csv'},
        {'zone': 'green', 'year': 2025, 'month': 11, 'file_type': 'parquet'},
    ]   

    for file in files:
        zone = file['zone']
        year = file['year']
        month = file['month']
        file_type = file['file_type']

        tripdata_url = get_tripdata_url(zone=zone, year=year, month=month)
        table_name = f"{zone}_taxi_data"
    
        etl_pipeline(
            url=tripdata_url,
            zone=zone,
            db_config=DB_CONFIG,
            table_name=table_name,
            chunk_size=CHUNK_SIZE,
            file_type=file_type
        )


if __name__ == "__main__":

    inspector = inspect(engine)
    YELLOW_TAXI_SCHEMA = utils.get_yellow_trip_schema()
    GREEN_TAXI_SCHEMA = utils.get_green_trip_schema()

    if inspector.has_table("taxi_zone_lookup"):        
        logger.info("Zone-Lookup table already exists. Skipping load.")
        pass
    else:
        zone_data_etl()
    
    if inspector.has_table("yellow_taxi_data"):
        logger.info("Yellow-Zone table already exists. Skipping table creation.")
        pass
    else:
        utils.create_table_if_not_exists(DB_CONFIG, "yellow_taxi_data", YELLOW_TAXI_SCHEMA)

    if inspector.has_table("green_taxi_data"):
        logger.info("Green-Zone table already exists. Skipping table creation.")
        pass
    else:
        utils.create_table_if_not_exists(DB_CONFIG, "green_taxi_data", GREEN_TAXI_SCHEMA)
 
        
    main()