CREATE MATERIALIZED VIEW mart.mv_patient_claim AS
SELECT 
    c.patient_id,
    t.year,
    t.month,
    SUM(coalesce(c.total,0)) AS total_klaim
FROM model.dim_claim c
JOIN model.dim_time t ON c.claim_time_id = t.time_id
GROUP BY c.patient_id, t.year, t.month
ORDER BY c.patient_id, t.year, t.month;