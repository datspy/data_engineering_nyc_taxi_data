import pandas as pd
import os
from sqlalchemy import text
import logging
import psycopg2
from typing import Iterator, Dict, Any
from psycopg2.extras import execute_batch


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def get_yellow_trip_schema() -> str:
    """Returns the SQL schema for the yellow taxi trip data table."""

    schema_sql = f"""
    CREATE TABLE yellow_taxi_data (
    	vendor_id INTEGER, 
    	pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
    	dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
    	passenger_count INTEGER, 
    	trip_distance_miles FLOAT(53), 
    	rate_code_id INTEGER, 
    	store_and_forward_flag TEXT, 
    	pickup_location_id INTEGER, 
    	dropoff_location_id INTEGER, 
    	payment_type INTEGER, 
    	fare_amount FLOAT(53), 
    	extra FLOAT(53), 
    	mta_tax FLOAT(53), 
    	tip_amount FLOAT(53), 
    	tolls_amount FLOAT(53), 
    	improvement_surcharge FLOAT(53), 
    	total_amount FLOAT(53), 
    	congestion_surcharge FLOAT(53), 
    	trip_duration_secs INTEGER
    )
    """

    return schema_sql



def get_green_trip_schema() -> str:
    """Returns the SQL schema for the green taxi trip data table."""

    schema_sql = f"""
    CREATE TABLE green_taxi_data (
        vendor_id INTEGER, 
        pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
        dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
        store_and_forward_flag TEXT, 
        rate_code_id INTEGER, 
        pickup_location_id INTEGER, 
        dropoff_location_id INTEGER, 
        passenger_count FLOAT(53), 
        trip_distance_miles FLOAT(53), 
        fare_amount FLOAT(53), 
        extra FLOAT(53), 
        mta_tax FLOAT(53), 
        tip_amount FLOAT(53), 
        tolls_amount FLOAT(53), 
        ehail_fee FLOAT(53), 
        improvement_surcharge FLOAT(53), 
        total_amount FLOAT(53), 
        payment_type FLOAT(53), 
        trip_type FLOAT(53), 
        congestion_surcharge FLOAT(53), 
        cbd_congestion_fee FLOAT(53), 
        trip_duration_secs INTEGER
    )
    """

    return schema_sql


def create_table_if_not_exists(db_config: Dict[str, Any], table_name: str, schema: str):
    """
    Create PostgreSQL table if it doesn't exist.
    
    Args:
        db_config: Database connection configuration
        table_name: Table name
        schema: CREATE TABLE SQL statement
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(schema)
        conn.commit()
        logger.info(f"Table '{table_name}' ready")
    except psycopg2.Error as e:
        logger.error(f"Failed to create table: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_to_postgres(df: pd.DataFrame, db_config: Dict[str, Any], table_name: str):
    """
    Load DataFrame chunk into PostgreSQL table.
    
    Args:
        df: Transformed DataFrame chunk
        db_config: Database connection configuration
        table_name: Target table name
    """
    logger.info(f"Loading {len(df)} rows to PostgreSQL table '{table_name}'")
    
    conn = None
    cursor = None
    
    try:
        # Establish connection
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Replace pandas NA/NaT with None and cast numpy scalars to Python types
        df = df.astype(object).where(pd.notna(df), None)

        # Prepare data for insertion
        columns = df.columns.tolist()
        values = df.to_numpy(dtype=object).tolist()
        
        # Create INSERT query
        placeholders = ','.join(['%s'] * len(columns))
        column_names = ','.join(columns)
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Execute batch insert
        execute_batch(cursor, insert_query, values, page_size=1000)
        
        # Commit transaction
        conn.commit()
        logger.info(f"Successfully loaded {len(df)} rows")
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def simple_load(engine, dataframe: pd.DataFrame, table_name: str, if_exists: str="replace"):
    """
    Loads a pandas DataFrame into a PostgreSQL database table.      
    """

    try:
        dataframe.to_sql(table_name, engine, index=False, if_exists=if_exists)
        logger.info(f"Successfully loaded {len(dataframe)} rows into '{table_name}'.")

    except Exception as e:
        logger.error(f"Failed to load data into Postgres: {e}")
        raise
    finally:
        engine.dispose()
        logger.info("Database connection closed!")
