import pandas as pd
from sqlalchemy import create_engine
import io
from dotenv import load_dotenv
import os
from utils import create_table_if_not_exists, load_to_postgres
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_URI = 'postgresql://pguser:pgpassword@localhost:5432/nyc_taxi'
engine = create_engine(DB_URI)

DB_CONFIG = {
    'host': 'localhost',
    'database': 'nyc_taxi',
    'user': 'pguser',
    'password': 'pgpassword',
    'port': 5432
}

zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def zone_data_etl(url=zone_url) -> pd.DataFrame:
    """Extract-Transform-Load the taxi zone lookup file
    Args:
        url (str): The URL of the taxi zone lookup CSV file.
    Returns:
        pd.DataFrame: The transformed DataFrame with renamed columns.
    """
    
    df_zone = pd.read_csv(url)

    df_zone.rename(columns={
                'LocationID': 'location_id',
                'Borough': 'borough',
                'Zone': 'zone',
                'service_zone': 'service_zone'
            }, inplace=True)
    
    tbl_name = 'taxi_zone_lookup'
    zone_schema = pd.io.sql.get_schema(df_zone, name=tbl_name, con=engine)
    create_table_if_not_exists(DB_CONFIG, tbl_name, zone_schema)    
    load_to_postgres(df_zone, DB_CONFIG, tbl_name)

    logger.info(f"Zone table loaded successfully. Total rows: {len(df_zone)}")
    
    return 1


if __name__ == "__main__":
    zone_data_etl()