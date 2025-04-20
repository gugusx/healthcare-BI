CREATE MATERIALIZED VIEW mart.mv_encounter_history AS
SELECT 
    location,
    service_provider,
    COUNT(encounter_id) AS total_encounters,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS avg_duration_minutes
FROM model.dim_encounter
WHERE end_time IS NOT NULL AND start_time IS NOT NULL
GROUP BY location, service_provider
ORDER BY total_encounters DESC;