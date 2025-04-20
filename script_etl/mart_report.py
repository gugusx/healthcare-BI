# %%
import logging
import configparser
import os
import sys

sys.path.append("/opt/airflow/script_etl")


from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from config import source_conn

config = configparser.ConfigParser()
config.read('manipulation.ini')

conn_str = f"postgresql://{source_conn['user']}:{source_conn['password']}@{source_conn['host']}:{source_conn['port']}/{source_conn['database']}"
engine = create_engine(conn_str)


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

def check_and_create_or_refresh_mv(engine, mv_name, mv_sql, schema="mart"):
    full_mv_name = f"{schema}.{mv_name}"
    try:
        with engine.connect() as conn:
            # Check if the materialized view exists
            check_sql = f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM pg_matviews 
                    WHERE schemaname = '{schema}' AND matviewname = '{mv_name}'
                );
            """
            exists = conn.execute(text(check_sql)).scalar()
            if exists:
                logging.info(f"Materialized view: {full_mv_name} already exists")
                # Refresh the materialized
                try:
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW {full_mv_name};"))
                    logging.info(f"Successfully refreshed materialized view: {full_mv_name}")
                except Exception as e:
                    logging.error(f"Error refreshing materialized view {full_mv_name}: {e}")
            else:
                logging.info(f"Creating materialized view: {full_mv_name}")
                conn.execute(text(mv_sql))
                #conn.commit()
                logging.info(f"Materialized view {full_mv_name} created successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error handling MV {mv_name}: {e}")


def run_mart_report():
    sql_folder = "/opt/airflow/mart_sql"

    # Iterate through all SQL files
    for file in os.listdir(sql_folder):
        if file.endswith(".sql"):
            mv_name = file.replace(".sql", "")
            with open(os.path.join(sql_folder, file), 'r') as f:
                sql_query = f.read()
                check_and_create_or_refresh_mv(engine, mv_name, sql_query, schema="mart")
    
    logging.info("Process Completed Successfully!")

# %%
if __name__ == "__main__":
    run_mart_report()


