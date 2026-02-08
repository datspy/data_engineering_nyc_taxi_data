import pandas as pd
import requests
from io import StringIO, BytesIO
from typing import Iterator, Dict, Any
from kestra import Kestra
import gzip
import argparse
import pyarrow.parquet as pq


# Configure logging
logger = Kestra.logger()

# Configuration
CHUNK_SIZE = 10000

DTYPE_INT_COLS = [
    "VendorID",
    "passenger_count",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "trip_type"
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
    "congestion_surcharge"
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("inp_file")
    parser.add_argument("zone", type=str)       
    return parser.parse_args()


def extract_parquet_chunks(pqtfile, chunk_size: int = CHUNK_SIZE) -> Iterator[pd.DataFrame]:
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
        # Read Parquet file
        parquet_table = pq.read_table(pqtfile)
        
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
            
    except Exception as e:
        logger.error(f"Failed to process Parquet file: {e}")
        raise


def extract_csv_chunks(csvfile, chunk_size: int = CHUNK_SIZE) -> Iterator[pd.DataFrame]:
    """
    Extract CSV data from file in chunks.
    """       
    
    chunk_iterator = pd.read_csv(
        csvfile,
        chunksize=chunk_size,
        encoding='utf-8',
        on_bad_lines='skip',
    )

    chunk_count = 0
    for chunk in chunk_iterator:
        chunk_count += 1
        logger.info(f"Extracted chunk {chunk_count} with {len(chunk)} rows")
        yield chunk


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


def etl_pipeline(input_file, zone: str, chunk_size: int = CHUNK_SIZE, file_type: str = 'csv'):
    """
    Execute the complete ETL pipeline.
    
    Args:
        file: Source File (CSV, CSV.GZ, or Parquet)
        zone: The zone of the trip data ('yellow' or 'green')
        chunk_size: Number of rows per chunk
        file_type: Type of file - 'csv' or 'parquet'
    """
    logger.info(f"Starting ETL pipeline for {file_type} file")   
   
    chunk_num = 0
    final_df = pd.DataFrame()  # Initialize an empty DataFrame to hold transformed data
    
    try:
        # Determine extraction method based on file type
        if file_type == 'parquet':
            chunk_iterator = extract_parquet_chunks(input_file, chunk_size)
        else:            
            chunk_iterator = extract_csv_chunks(input_file, chunk_size)
        
        # Process each chunk
        for chunk in chunk_iterator:
            chunk_num += 1
            
            # Transform
            transformed_chunk = transform_data(chunk, zone=zone)
            final_df = pd.concat([final_df, transformed_chunk], ignore_index=True)

        final_df = final_df.reset_index(drop=True)  # Reset index after concatenation        
        logger.info(f"ETL pipeline completed successfully. Total rows processed: {len(final_df)}")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise
    
    return final_df


def main():

    args = parse_args() 
    inp_file = args.inp_file
    zone = args.zone
    logger.info(f"Processing {zone} zone data!!")
    op_filename = f"{zone}_tripdata_transformed.csv"
    df = etl_pipeline(input_file=inp_file,zone=zone)
    df.to_csv(op_filename, index=False)
    logger.info(f"Finished writing output file: {op_filename} !!")


if __name__ == "__main__":
        
    main()