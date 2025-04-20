CREATE MATERIALIZED VIEW mart.mv_most_diagnose AS
SELECT 
    remark as category,
    COUNT(*) AS total_tercatat
FROM model.dim_condition where remark != ''
GROUP BY remark
ORDER BY total_tercatat DESC;