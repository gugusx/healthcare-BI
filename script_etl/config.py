#!python config

import os

from dotenv import load_dotenv

# Load environment variables from the .env file
dotenv_path = "/opt/airflow/script_etl/.env"
load_dotenv(dotenv_path=dotenv_path)

# PostgreSQL config
source_conn = {
    'user': os.getenv('USER_POSTGRES'),
    'password': os.getenv('PASS_POSTGRES'),
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT'),
    'database': os.getenv('DB')
}

########## MAPPING FOR STAGING ##########
staging_mappings = {
    "condition.csv": {
        "ID": "condition_id",
        "Clinical Status": "clinical_status",
        "Verification Status": "verification_status",
        "Category": "category",
        "Remark": "remark",
        "ID Patient": "patient_id",
        "ID Encounter": "encounter_id",
        "On Set Date": "onsetDateTime",
        "Recorded Date": "recordedDate"
    },
    "encounter.csv": {
        "ID": "encounter_id",
        "Status": "status",
        "Class Code": "class_code",
        "Encounter Type": "encounter_type",
        "ID Patient": "patient_id",
        "Doctor Name": "doctor_name",
        "Doctor Role": "doctor_role",
        "Start Encounter": "start_time",
        "End Encounter": "end_time",
        "Location": "location",
        "Service Provider": "service_provider"
    },
    "patient.csv": {
        "ID": "patient_id",
        "First Name": "firstname",
        "Last Name": "lastname",
        "Gender": "gender",
        "Birth Date": "birthdate",
        "Marital Status": "marital_status",
        "Phone": "phone",
        "City": "city",
        "State": "state",
        "Country": "country"
    },
    "claim.csv": {
        "ID": "claim_id",
        "Status": "status",
        "Claim Type": "claim_type",
        "ID Patient": "patient_id",
        "billablePeriod Start": "billablePeriod_start",
        "billablePeriod End": "billablePeriod_end",
        "Created Date": "created_date",
        "ID Encounter": "encounter_id",
        "Total": "total",
        "Currency": "currency"
    }
}


staging_data_types = {
    "condition.csv": {
        "condition_id": "VARCHAR(100)",
        "clinical_status": "VARCHAR(30)",
        "verification_status": "VARCHAR(30)",
        "category": "VARCHAR(100)",
        "remark": "VARCHAR(100)",
        "patient_id": "VARCHAR(100)",
        "encounter_id": "VARCHAR(100)",
        "onsetDateTime": "TIMESTAMP",
        "recordedDate": "TIMESTAMP"
    },
    "encounter.csv": {
        "encounter_id": "VARCHAR(100)",
        "status": "VARCHAR(30)",
        "class_code": "VARCHAR(10)",
        "encounter_type": "VARCHAR(100)",
        "patient_id": "VARCHAR(100)",
        "doctor_name": "VARCHAR(100)",
        "doctor_role": "VARCHAR(50)",
        "start_time": "TIMESTAMP",
        "end_time": "TIMESTAMP",
        "location": "VARCHAR(100)",
        "service_provider": "VARCHAR(100)"
    },
    "patient.csv": {
        "patient_id": "VARCHAR(100)",
        "firstname": "VARCHAR(100)",
        "lastname": "VARCHAR(100)",
        "gender": "VARCHAR(10)",
        "birthdate": "DATE",
        "marital_status": "VARCHAR(30)",
        "phone": "VARCHAR(20)",
        "city": "VARCHAR(30)",
        "state": "VARCHAR(10)",
        "country": "VARCHAR(10)"
    },
    "claim.csv": {
        "claim_id": "VARCHAR(100)",
        "status": "VARCHAR(30)",
        "claim_type": "VARCHAR(30)",
        "patient_id": "VARCHAR(100)",
        "billablePeriod_start": "TIMESTAMP DEFAULT NULL",
        "billablePeriod_end": "TIMESTAMP DEFAULT NULL",
        "created_date": "TIMESTAMP DEFAULT NULL",
        "encounter_id": "VARCHAR(100)",
        "total": "NUMERIC (10,2)",
        "currency": "VARCHAR(10)"
    }
}
######## END MAPPING FOR STAGING ########


############ MAPPING FOR DWH ############
dwh_mappings = {
    "patient": {
        "source_table": "raw.patient",
        "destination_table": "dim_patient",
        "column_mapping": {
            "patient_id": "patient_id",
            "firstname": "firstname",
            "lastname": "lastname",
            "gender": "gender",
            "birthdate": "birthdate",
            "marital_status": "marital_status",
            "phone": "phone",
            "city": "city",
            "state": "state",
            "country": "country"
        },
        "query": """
            SELECT *
            FROM raw.patient
        """
    },
    "condition": {
        "source_table": "raw.condition",
        "destination_table": "dim_condition",
        "column_mapping": {
            "condition_id": "condition_id",
            "clinical_status": "clinical_status",
            "verification_status": "verification_status",
            "category": "category",
            "remark": "remark",
            "patient_id": "patient_id",
            "encounter_id": "encounter_id",
            "onsetDateTime": "onsetDateTime",
            "recordedDate": "recordedDate",
            "condition_time_id": "condition_time_id"
        },
        "query": """
            SELECT c.*, time_id as condition_time_id
            FROM raw.condition c
            LEFT JOIN model.dim_time dt
            ON date(c.onsetDateTime) = dt.date
        """
    },
    "encounter": {
        "source_table": "raw.encounter",
        "destination_table": "dim_encounter",
        "column_mapping": {
            "encounter_id": "encounter_id",
            "status": "status",
            "class_code": "class_code",
            "encounter_type": "encounter_type",
            "patient_id": "patient_id",
            "start_time": "start_time",
            "end_time": "end_time",
            "location": "location",
            "service_provider": "service_provider",
            "encounter_time_id": "encounter_time_id"
        },
        "query": """
            SELECT encounter_id, status, class_code, encounter_type, patient_id, start_time, end_time, location, service_provider, time_id as encounter_time_id
            FROM raw.encounter e
            LEFT JOIN model.dim_time dt
            ON date(e.start_time) = dt.date
        """
    },
    "claim": {
        "source_table": "raw.claim",
        "destination_table": "dim_claim",
        "column_mapping": {
            "claim_id": "claim_id",
            "status": "status",
            "claim_type": "claim_type",
            "patient_id": "patient_id",
            "billablePeriod_start": "billablePeriod_start",
            "billablePeriod_end": "billablePeriod_end",
            "created_date": "created_date",
            "encounter_id": "encounter_id",
            "total": "total",
            "currency": "currency",
            "claim_time_id": "claim_time_id"
        },
        "query": """
            SELECT c.*, time_id as claim_time_id
            FROM raw.claim c
            LEFT JOIN model.dim_time dt
            ON date(c.created_date) = dt.date
        """
    },
    "medical_record": {
        "source_table": ["raw.patient", "raw.condition", "raw.encounter", "raw.claim", "model.dim_time"],
        "destination_table": "fact_medical_record",
        "column_mapping": {
            "patient_id": "patient_id",
            "condition_id": "condition_id",
            "encounter_id": "encounter_id",
            "claim_id": "claim_id",
            "total_claim_amount": "total_claim_amount",
            "total_encounters": "total_encounters",
            "total_conditions": "total_conditions",
            "encounter_time_id": "encounter_time_id",
            "claim_time_id": "claim_time_id",
            "condition_time_id": "condition_time_id"
        },
        "query": """
            SELECT 
                p.patient_id,
                c.condition_id,
                e.encounter_id,
                cl.claim_id,
                SUM(cl.total) AS total_claim_amount,
                COUNT(DISTINCT e.encounter_id) AS total_encounters,
                COUNT(DISTINCT c.condition_id) AS total_conditions,
                dt_enc.time_id AS encounter_time_id,
                dt_cl.time_id AS claim_time_id,
                dt_c.time_id AS condition_time_id
            FROM raw.patient p
            LEFT JOIN raw.condition c ON p.patient_id = c.patient_id
            LEFT JOIN raw.encounter e ON p.patient_id = e.patient_id
            LEFT JOIN raw.claim cl ON p.patient_id = cl.patient_id
            LEFT JOIN model.dim_time dt_c ON DATE(c.onsetDateTime) = dt_c.date
            LEFT JOIN model.dim_time dt_enc ON DATE(e.start_time) = dt_enc.date
            LEFT JOIN model.dim_time dt_cl ON DATE(cl.created_date) = dt_cl.date
            GROUP BY p.patient_id, c.condition_id, e.encounter_id, cl.claim_id, dt_enc.time_id, dt_cl.time_id, dt_c.time_id
        """
    }
}
