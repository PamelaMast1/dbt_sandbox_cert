{{ config(
    materialized = 'table',
    schema = 'meta'
) }}

SELECT CAST(NULL AS string) AS seed_name,
       CAST(NULL AS TIMESTAMP) AS loaded_at,
       CAST(NULL AS int64) AS row_count,
       CAST(NULL AS string) AS run_id
FROM (SELECT 1) AS dummy
WHERE 1 = 0