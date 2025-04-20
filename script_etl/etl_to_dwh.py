# %% [markdown]
# #### Extract JSON

# %%
import json
import os
import pandas as pd
import logging
import sys


# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Logs to console
        #logging.FileHandler('application.log')  # Logs to a file
    ]
)

folder_path = '/opt/airflow/synthea_sample_data_fhir_latest/'

cleaned_dir = '/opt/airflow/cleaned/'

if not os.path.exists(cleaned_dir):
    os.makedirs(cleaned_dir)

patient_csv = os.path.join(cleaned_dir, 'patient.csv')
encounter_csv = os.path.join(cleaned_dir, 'encounter.csv')
condition_csv = os.path.join(cleaned_dir, 'condition.csv')
claim_csv = os.path.join(cleaned_dir, 'claim.csv')

#patient_csv = 'patient.csv'
#encounter_csv = 'encounter.csv'
#condition_csv = 'condition.csv'
#claim_csv = 'claim.csv'

patient_data = []
encounter_data = []
condition_data = []
claim_data = []

def extract_data():
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            
            # Read JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
    
            # 1. Extract patient info
            logging.info(f"Extract data {filename}...")
            if "entry" in data and isinstance(data["entry"], list) and len(data["entry"]) > 0:
                patient_info = data["entry"][0]["resource"]
    
                patient_id = patient_info.get("id", None)
    
                name_info = patient_info.get("name", [])
                if isinstance(name_info, list) and len(name_info) > 0:
                    firstname = " ".join(name_info[0].get("given", [])) if isinstance(name_info[0].get("given", []), list) else name_info[0].get("given", "")
                    lastname = name_info[0].get("family", "")
                else:
                    firstname, lastname = None, None
                    
                gender = patient_info.get("gender", None)
                birthdate = patient_info.get("birthDate", None)
    
                marital_status_info = patient_info.get("maritalStatus", {})
                marital_status = marital_status_info.get("text", None) if isinstance(marital_status_info, dict) else None
                
                telecom_info = patient_info.get("telecom", [])
                phone = telecom_info[0].get("value", None) if isinstance(telecom_info, list) and len(telecom_info) > 0 else None
    
                address_info = patient_info.get("address", [])
                if isinstance(address_info, list) and len(address_info) > 0:
                    city = address_info[0].get("city", None)
                    state = address_info[0].get("state", None)
                    country = address_info[0].get("country", None)
                else:
                    city, state, country = None, None, None
                
                # Append to list
                patient_data.append([patient_id,
                                    firstname,
                                    lastname,
                                    gender,
                                    birthdate,
                                    marital_status,
                                    phone,
                                    city,
                                    state,
                                    country])
    
                
                # 2. Extract encounter info
                encounter_info = data["entry"][1]["resource"]
                
                encounter_id = encounter_info.get("id", None)
                status = encounter_info.get("status", None)
                class_code = encounter_info.get("class", {}).get("code", None)
                encounter_type = encounter_info.get("type", [{}])[0].get("text", None)
        
                patient_id = None
                if "subject" in encounter_info and "reference" in encounter_info["subject"]:
                    patient_id = encounter_info["subject"]["reference"].split(":")[-1]
        
                doctor_name = None
                doctor_role = None
                if "participant" in encounter_info and len(encounter_info["participant"]) > 0:
                    participant = encounter_info["participant"][0]
                    doctor_name = participant.get("individual", {}).get("display", None)
                    doctor_role = participant.get("type", [{}])[0].get("text", None)
        
                start_time = encounter_info.get("period", {}).get("start", None)
                end_time = encounter_info.get("period", {}).get("end", None)
        
                location = None
                if "location" in encounter_info and len(encounter_info["location"]) > 0:
                    location = encounter_info["location"][0].get("location", {}).get("display", None)
        
                service_provider = encounter_info.get("serviceProvider", {}).get("display", None)
    
                encounter_data.append([encounter_id,
                                       status,
                                       class_code,
                                       encounter_type,
                                       patient_id,
                                       doctor_name,
                                       doctor_role,
                                       start_time,
                                       end_time,
                                       location,
                                       service_provider])
    
                
                # 3. Extract condition info
                condition_info = data["entry"][2]["resource"]
                
                condition_id = condition_info.get("id", None)
                clinical_status = condition_info.get("clinicalStatus", {}).get("coding", [{}])[0].get("code", None)
                verification_status = condition_info.get("verificationStatus", {}).get("coding", [{}])[0].get("code", None)
    
                try:
                    category = condition_info["category"][0]["coding"][0]["display"]
                except (KeyError, IndexError, TypeError):
                    category = None
                
                remark = condition_info.get("code", {}).get("coding", [{}])[0].get("display", None)
        
                patient_id = None
                if "subject" in condition_info and "reference" in condition_info["subject"]:
                    patient_id = condition_info["subject"]["reference"].split(":")[-1]
        
                encounter_id = None
                if "encounter" in condition_info and "reference" in condition_info["encounter"]:
                    encounter_id = condition_info["encounter"]["reference"].split(":")[-1]
        
                onsetDateTime = condition_info.get("onsetDateTime", None)
                recordedDate = condition_info.get("recordedDate", None)
    
                condition_data.append([condition_id,
                                       clinical_status,
                                       verification_status,
                                       category,
                                       remark,
                                       patient_id,
                                       encounter_id,
                                       onsetDateTime,
                                       recordedDate])
    
                
                # 4. Extract claim info
                claim_info = data["entry"][6]["resource"]
           
                claim_id = claim_info.get("id", None)
                status = claim_info.get("status", None)
    
                claim_type = None
                if "type" in claim_info and isinstance(claim_info["type"], dict):
                    claim_type = claim_info["type"].get("coding", [{}])[0].get("code", None)
    
                patient_id = claim_info.get("patient", {}).get("reference", "").split(":")[-1]
    
                billablePeriod_start = claim_info.get("billablePeriod", {}).get("start", None)
                billablePeriod_end = claim_info.get("billablePeriod", {}).get("end", None)
                
                created_date = claim_info.get("created", None)
                
                encounter_id = None
                if "item" in claim_info and isinstance(claim_info["item"], list):
                    for item in claim_info["item"]:
                        if "encounter" in item and isinstance(item["encounter"], list) and len(item["encounter"]) > 0:
                            encounter_id = item["encounter"][0]["reference"].split(":")[-1]
                            break  
                
                total_info = claim_info.get("total", {})
                if isinstance(total_info, dict):  # Jika dictionary
                    total = total_info.get("value", None)
                    currency = total_info.get("currency", None)
                elif isinstance(total_info, list) and len(total_info) > 0:  # Jika list
                    total = total_info[0].get("value", None)
                    currency = total_info[0].get("currency", None)
                else:  # Jika tidak ditemukan
                    total = None
                    currency = None
    
                claim_data.append([claim_id,
                                    status,
                                    claim_type,
                                    patient_id,
                                    billablePeriod_start,
                                    billablePeriod_end,
                                    created_date,
                                    encounter_id,
                                    total,
                                    currency])


def store_data():
    df = pd.DataFrame(patient_data, columns=['ID', 'First Name', 'Last Name', 'Gender', 'Birth Date', 'Marital Status', 'Phone', 'City', 'State', 'Country'])
    df.to_csv(patient_csv, index=False)
    logging.info(f"Successfully saved {len(df)} patients to {patient_csv}")
    
    df_e = pd.DataFrame(encounter_data, columns=['ID', 'Status', 'Class Code', 'Encounter Type', 'ID Patient', 'Doctor Name', 'Doctor Role', 'Start Encounter', 'End Encounter', 'Location', 'Service Provider'])
    df_e.to_csv(encounter_csv, index=False)
    logging.info(f"Successfully saved {len(df)} encounters to {encounter_csv}")
    
    df_c = pd.DataFrame(condition_data, columns=['ID', 'Clinical Status', 'Verification Status', 'Category', 'Remark', 'ID Patient', 'ID Encounter', 'On Set Date', 'Recorded Date'])
    df_c.to_csv(condition_csv, index=False)
    logging.info(f"Successfully saved {len(df)} conditions to {condition_csv}")
    
    df_cl = pd.DataFrame(claim_data, columns=['ID', 'Status', 'Claim Type', 'ID Patient', 'billablePeriod Start', 'billablePeriod End', 'Created Date', 'ID Encounter', 'Total', 'Currency'])
    df_cl.to_csv(claim_csv, index=False)
    logging.info(f"Successfully saved {len(df)} claims to {claim_csv}")

# %% [markdown]
# #### Transform

# %%
import pandas as pd

csv_files = [patient_csv, encounter_csv, condition_csv, claim_csv]

def transform_data():
    datetime_columns = {
        encounter_csv: ["Start Encounter", "End Encounter"],
        condition_csv: ["On Set Date", "Recorded Date"],
        claim_csv: ["billablePeriod Start", "billablePeriod End", "Created Date"]
    }

    output_directory = "/opt/airflow/transformed/"
    os.makedirs(output_directory, exist_ok=True)
    
    for file in csv_files:
        print(f"Processing {file}...")
    
        df = pd.read_csv(file)

        # 1. Drop Duplicate
        logging.info(f"Drop duplication in {file}...")
        df = df.drop_duplicates()

        # 2. Change Date Format
        for dt_col in datetime_columns.get(file, []):
            logging.info(f"Change data format for column {dt_col}...")
            df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
            df[dt_col] = df[dt_col].dt.tz_localize(None)
        
        transformed_file_path = os.path.join(output_directory, f"{file}")

        # Save result
        df.to_csv(transformed_file_path, index=False)
        logging.info(f"Transformed file saved as: {transformed_file_path}")
    
    logging.info("Transformation completed.")

# %% [markdown]
# #### Load data to Staging

# %%
import os
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine

sys.path.append("/opt/airflow/script_etl")

# %%
from config import source_conn
from config import staging_mappings
from config import staging_data_types

logging.info(f"source_conn = {source_conn}")

conn_str = f"postgresql://{source_conn['user']}:{source_conn['password']}@{source_conn['host']}:{source_conn['port']}/{source_conn['database']}"
engine = create_engine(conn_str)

# Helper to extract just the base type (e.g., "TIMESTAMP" from "TIMESTAMP DEFAULT NULL")
def get_base_type(sql_type: str):
    return sql_type.split()[0].upper()

def load_csv(csv_folder: str, schema: str = "staging"):
    """Load All CSV to Staging"""
    
    files = [f for f in os.listdir(csv_folder) if f.endswith(".csv")]

    for file in files:
        logging.info(f"Processing {file}...")
        logging.info(f"Load {file} to Staging schema...")

        file_path = os.path.join(csv_folder, file)
        df = pd.read_csv(file_path)  # Load CSV
        df = df.rename(columns=staging_mappings[file]) # Rename Column
        #df = df.rename(columns=staging_mappings.get(file, {}))

        # Apply data types from staging_data_types
        if file in staging_data_types:
            types = staging_data_types[file]
            for col, dtype in types.items():
                if col in df.columns:
                    base_type = get_base_type(dtype)

                    # Convert based on basic SQL types
                    if base_type in ["TIMESTAMP", "DATE"]:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    elif base_type.startswith("NUMERIC"):
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    elif base_type.startswith("VARCHAR"):
                        df[col] = df[col].fillna("").astype(str)
        
        df.replace("nan", "", inplace=True)

        # Write to DB
        table_name = file.replace(".csv", "").lower()
        df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
        logging.info(f"Loaded {file} into {schema}.{table_name} ({len(df)} rows)\n")

# %% [markdown]
# #### Delete+Insert in Staging and Load to Raw schema

# %%
import configparser
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from config import source_conn

def load_queries(config_file='/opt/airflow/script_etl/manipulation.ini'):
    """Loads queries from config file"""
    
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['queries_filtering']


def execute_queries(queries):
    """Executes a list of SQL queries"""
    #conn_url = f"postgresql://{source_conn['user']}:{quote_plus(source_conn['password'])}@{source_conn['host']}:{source_conn['port']}/{source_conn['database']}"
    conn_url = f"postgresql+psycopg2://{source_conn['user']}:{quote_plus(source_conn['password'])}@{source_conn['host']}:{source_conn['port']}/{source_conn['database']}"
    engine = create_engine(conn_url)

    try:
        logging.info("Starting query execution.")
        with engine.begin() as conn:
            for query_name, query in queries.items():
                logging.info(f"Executing query: {query_name}")
                conn.execute(text(query))
                logging.info(f"Query executed successfully: {query_name}")
    except Exception as e:
        logging.error(f"Error occurred: {e}")

def main():
    queries = load_queries()
    execute_queries(queries)

# %% [markdown]
# #### Run All Process

# %%
def run_etl_to_dwh():
    extract_data()
    store_data()
    transform_data()
    load_csv("/opt/airflow/transformed/")
    main()
    logging.info("ETL Process Completed Successfully!")

# %%
if __name__ == "__main__":
    run_etl_to_dwh()


