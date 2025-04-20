CREATE DATABASE healthcare;


CREATE schema staging;
CREATE schema raw;
CREATE schema model;
CREATE schema mart;


---------- STAGING ----------
CREATE TABLE IF NOT EXISTS staging.patient (
    patient_id VARCHAR(100),
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    gender VARCHAR(10),
    birthdate DATE DEFAULT NULL,
    marital_status VARCHAR(30),
    phone VARCHAR(100),
    city VARCHAR(30),
    state VARCHAR(10),
    country VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS staging.encounter (
    encounter_id VARCHAR(100),
    status VARCHAR(30),
    class_code VARCHAR(10),
    encounter_type VARCHAR(100),
    patient_id VARCHAR(100),
    doctor_name VARCHAR(100),
    doctor_role VARCHAR(50),
    start_time TIMESTAMP DEFAULT NULL,
    end_time TIMESTAMP DEFAULT NULL,
    location VARCHAR(100),
    service_provider VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS staging.condition (
    condition_id VARCHAR(100),
    clinical_status VARCHAR(30),
    verification_status VARCHAR(30),
    category VARCHAR(100),
    remark VARCHAR(100),
    patient_id VARCHAR(100),
    encounter_id VARCHAR(100),
    onsetDateTime TIMESTAMP DEFAULT NULL,
    recordedDate TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS staging.claim (
    claim_id VARCHAR(100),
    status VARCHAR(30),
    claim_type VARCHAR(30),
    patient_id VARCHAR(100),
    billablePeriod_start TIMESTAMP DEFAULT NULL,
    billablePeriod_end TIMESTAMP DEFAULT NULL,
    created_date TIMESTAMP DEFAULT NULL,
    encounter_id VARCHAR(100),
    total NUMERIC (10,2),
    currency VARCHAR(10)
);
-------- END STAGING --------


------------ RAW ------------
CREATE TABLE IF NOT EXISTS raw.patient (
    patient_id VARCHAR(100),
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    gender VARCHAR(10),
    birthdate DATE DEFAULT NULL,
    marital_status VARCHAR(30),
    phone VARCHAR(100),
    city VARCHAR(30),
    state VARCHAR(10),
    country VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS raw.encounter (
    encounter_id VARCHAR(100),
    status VARCHAR(30),
    class_code VARCHAR(10),
    encounter_type VARCHAR(100),
    patient_id VARCHAR(100),
    doctor_name VARCHAR(100),
    doctor_role VARCHAR(50),
    start_time TIMESTAMP DEFAULT NULL,
    end_time TIMESTAMP DEFAULT NULL,
    location VARCHAR(100),
    service_provider VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw.condition (
    condition_id VARCHAR(100),
    clinical_status VARCHAR(30),
    verification_status VARCHAR(30),
    category VARCHAR(100),
    remark VARCHAR(100),
    patient_id VARCHAR(100),
    encounter_id VARCHAR(100),
    onsetDateTime TIMESTAMP DEFAULT NULL,
    recordedDate TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS raw.claim (
    claim_id VARCHAR(100),
    status VARCHAR(30),
    claim_type VARCHAR(30),
    patient_id VARCHAR(100),
    billablePeriod_start TIMESTAMP DEFAULT NULL,
    billablePeriod_end TIMESTAMP DEFAULT NULL,
    created_date TIMESTAMP DEFAULT NULL,
    encounter_id VARCHAR(100),
    total NUMERIC (10,2),
    currency VARCHAR(10)
);
---------- END RAW ----------


----------- MODEL -----------
CREATE TABLE IF NOT EXISTS model.dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    day_of_week VARCHAR(10),
    is_weekend BOOLEAN
);
-- Generate Date range for 24 years
INSERT INTO model.dim_time (date, year, quarter, month, week, day, day_of_week, is_weekend)
SELECT 
    date_series AS date,
    EXTRACT(YEAR FROM date_series) AS year,
    EXTRACT(QUARTER FROM date_series) AS quarter,
    EXTRACT(MONTH FROM date_series) AS month,
    EXTRACT(WEEK FROM date_series) AS week,
    EXTRACT(DAY FROM date_series) AS day,
    TO_CHAR(date_series, 'Day') AS day_of_week,
    CASE WHEN EXTRACT(ISODOW FROM date_series) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2000-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) date_series;

CREATE TABLE IF NOT EXISTS model.dim_patient (
    patient_id VARCHAR(100) PRIMARY KEY,
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    gender VARCHAR(10),
    birthdate DATE DEFAULT NULL,
    marital_status VARCHAR(30),
    phone VARCHAR(100),
    city VARCHAR(30),
    state VARCHAR(10),
    country VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS model.dim_encounter (
    encounter_id VARCHAR(100) PRIMARY KEY,
    status VARCHAR(30),
    class_code VARCHAR(10),
    encounter_type VARCHAR(100),
    patient_id VARCHAR(100),
    start_time TIMESTAMP DEFAULT NULL,
    end_time TIMESTAMP DEFAULT NULL,
    location VARCHAR(100),
    service_provider VARCHAR(100),
    encounter_time_id INT
);

CREATE TABLE IF NOT EXISTS model.dim_condition (
    condition_id VARCHAR(100) PRIMARY KEY,
    clinical_status VARCHAR(30),
    verification_status VARCHAR(30),
    category VARCHAR(100),
    remark VARCHAR(100),
    patient_id VARCHAR(100),
    encounter_id VARCHAR(100),
    onsetDateTime TIMESTAMP DEFAULT NULL,
    recordedDate TIMESTAMP DEFAULT NULL,
    condition_time_id INT
);

CREATE TABLE IF NOT EXISTS model.dim_claim (
    claim_id VARCHAR(100) PRIMARY KEY,
    status VARCHAR(30),
    claim_type VARCHAR(30),
    patient_id VARCHAR(100),
    billablePeriod_start TIMESTAMP DEFAULT NULL,
    billablePeriod_end TIMESTAMP DEFAULT NULL,
    created_date TIMESTAMP DEFAULT NULL,
    encounter_id VARCHAR(100),
    total NUMERIC (10,2),
    currency VARCHAR(10),
    claim_time_id INT
);

CREATE TABLE IF NOT EXISTS model.fact_medical_record (
	medical_record_id SERIAL NOT NULL PRIMARY KEY,
	patient_id VARCHAR(100),
	condition_id VARCHAR(100),
	encounter_id VARCHAR(100),
	claim_id VARCHAR(100),
	total_claim_amount NUMERIC(10,2),
    total_encounters INT, 
    total_conditions INT,
    encounter_time_id INT,
    claim_time_id INT,
    condition_time_id INT
);

CREATE INDEX idx_fmr_patient ON model.fact_medical_record(patient_id);
CREATE INDEX idx_fmr_condition ON model.fact_medical_record(condition_id);
CREATE INDEX idx_fmr_encounter ON model.fact_medical_record(encounter_id);
CREATE INDEX idx_fmr_claim ON model.fact_medical_record(claim_id);
CREATE INDEX idx_fmr_et ON model.fact_medical_record(encounter_time_id);
CREATE INDEX idx_fmr_ct ON model.fact_medical_record(claim_time_id);
CREATE INDEX idx_fmr_cont ON model.fact_medical_record(condition_time_id);
--------- END MODEL ---------

