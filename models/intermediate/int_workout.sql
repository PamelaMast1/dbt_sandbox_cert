{{ config(materialized="view") }} 
-- just added to investigate how acces / groups work
SELECT * 
FROM {{ ref("stg_workout") }}
