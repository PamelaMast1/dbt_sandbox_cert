{{ config(severity='warn') }}

WITH by_day AS (
    SELECT SUM(total_output) as total_output
    FROM {{ ref('agg_workout_by_day') }}
)

SELECT 1 AS r
FROM by_day d