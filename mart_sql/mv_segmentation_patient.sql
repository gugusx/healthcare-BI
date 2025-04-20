CREATE MATERIALIZED VIEW mart.mv_segmentation_patient AS
WITH patient_age AS (
    SELECT 
        p.patient_id,
        EXTRACT(YEAR FROM AGE(current_date, p.birthdate)) AS age,
        p.marital_status
    FROM model.dim_patient p
),
encounter_count AS (
    SELECT 
        patient_id,
        COUNT(*) AS total_encounters
    FROM model.dim_encounter
    GROUP BY patient_id
),
condition_count AS (
    SELECT 
        patient_id,
        COUNT(*) AS total_conditions
    FROM model.dim_condition
    GROUP BY patient_id
)
SELECT marital_status, count(case when category = '0-17' then age end) as "0-17", count(case when category = '18-35' then age end) as "18-35", count(case when category = '36-60' then age end) as "36-60", count(case when category = '>60' then age end) as "60+", count(age) as total_patient, 
sum(case when category = '0-17' then total_encounters end) as "Total Encounter (Age: 0-17)", sum(case when category = '18-35' then total_encounters end) as "Total Encounter (Age: 18-35)", sum(case when category = '36-60' then total_encounters end) as "Total Encounter (Age: 36-60)", sum(case when category = '>60' then total_encounters end) as "Total Encounter (Age: 60+)", sum(total_encounters) as total_encounters,
sum(case when category = '0-17' then total_conditions end) as "Total Condition (Age: 0-17)", sum(case when category = '18-35' then total_conditions end) as "Total Condition (Age: 18-35)", sum(case when category = '36-60' then total_conditions end) as "Total Condition (Age: 36-60)", sum(case when category = '>60' then total_conditions end) as "Total Condition (Age: 60+)", sum(total_conditions) as total_conditions
FROM(
SELECT 
age,
    case when age < 18 then '0-17' when age between 18 AND 35 then '18-35' when age between 36 AND 60 then '36-60' else '>60' end as category,
    marital_status,
    COALESCE(e.total_encounters, 0) AS total_encounters,
    COALESCE(c.total_conditions, 0) AS total_conditions
FROM patient_age pa
LEFT JOIN encounter_count e ON pa.patient_id = e.patient_id
LEFT JOIN condition_count c ON pa.patient_id = c.patient_id
WHERE age is NOT NULL) as s
GROUP BY marital_status;