# %%
import logging
import pandas as pd
import sys
import os

from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

sys.path.append("/opt/airflow/script_etl")

from config import source_conn
from config import dwh_mappings


import warnings
warnings.filterwarnings('ignore')

# %% [markdown]
# #### Function

# %%
# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Logs to console
        #logging.FileHandler('application.log')  # Logs to a file
    ]
)

def db_connection(conn_params):
    """Create a connection engine to the database"""
    conn_str = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    engine = create_engine(conn_str)
    return engine.connect()

def validate_config(dwh_mappings):
    """Validate the ETL configuration"""
    required_keys = ['source_table', 'query', 'destination_table', 'column_mapping']
    for table_name, table_config in dwh_mappings.items():
        for key in required_keys:
            if key not in table_config:
                raise ValueError(f"Missing {key} in config for table {table_name}")
    logging.info("Config validation passed")

def extract(table_config):
    """Extract data from the source table"""
    try:
        logging.info(f"Extracting data from {table_config['source_table']}...")
        with db_connection(source_conn) as conn:
            df = pd.read_sql(table_config["query"], conn)
        return df
    except Exception as e:
        logging.error(f"Error extracting data from {table_config['source_table']}: {e}")
        raise

def transform(df, table_config):
    """Transform the extracted data"""
    try:
        logging.info(f"Transforming data for {table_config['destination_table']}...")
        df.rename(columns=table_config["column_mapping"], inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error transforming data for {table_config['destination_table']}: {e}")
        raise

def load(df, table_config):
    """Load the transformed data into the destination table, replacing the data without dropping the table"""
    try:
        logging.info(f"Replacing data in {table_config['destination_table']}...")

        # Connect to the warehouse database
        with db_connection(source_conn) as conn:
            conn.execute(text(f"TRUNCATE TABLE model.{table_config['destination_table']} RESTART IDENTITY CASCADE;"))
            #conn.commit()

            # Step 2: Insert new data
            df.to_sql(
                table_config["destination_table"], 
                con=conn,
                if_exists="append",  
                index=False,
                schema="model"
            )

        logging.info(f"Data successfully loaded into {table_config['destination_table']}")
    except Exception as e:
        logging.error(f"Error replacing data in {table_config['destination_table']}: {e}")
        raise


def run_dwh_to_mart():
    """Run the full ETL process."""
    try:
        logging.info("Starting ETL Process...")
        validate_config(dwh_mappings)
        for table_name, table_config in dwh_mappings.items():
            df = extract(table_config)
            df = transform(df, table_config)
            load(df, table_config)
        logging.info("ETL Process Completed Successfully!")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")

# %% [markdown]
# #### Run All Process

# %%
if __name__ == "__main__":
    run_dwh_to_mart()


