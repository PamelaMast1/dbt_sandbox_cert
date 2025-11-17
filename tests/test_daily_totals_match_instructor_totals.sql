{{ config(severity='warn') }}

WITH by_day AS (
    SELECT SUM(total_output) as total_output
    FROM {{ ref('agg_workout_by_day') }}
),

by_instructor AS (
    SELECT SUM(total_output) as total_output
    FROM {{ ref('agg_workout_by_instructor') }}
)

SELECT 1 AS r
FROM by_day d
INNER JOIN by_instructor i ON d.total_output = i.total_output