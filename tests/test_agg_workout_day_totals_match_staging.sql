{{ config(severity='warn') }}

WITH stg AS (
    SELECT DATE(workout_timestamp) AS workout_date,
           SUM(total_output) AS total_output
    FROM {{ ref('int_workout') }}
    GROUP BY workout_date
),

agg AS (
    SELECT workout_date,
           total_output
    FROM {{ ref('agg_workout_by_day') }}
)

SELECT agg.workout_date
FROM agg
LEFT JOIN stg ON agg.workout_date = stg.workout_date
WHERE agg.total_output != stg.total_output