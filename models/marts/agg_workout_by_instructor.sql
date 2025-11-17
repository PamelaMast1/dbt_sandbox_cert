{{ config(materialized='table') }}

SELECT instructor,
       COUNT(*) AS workout_count,
       SUM(calories_burned) AS total_calories,
       SUM(total_output) AS total_output
FROM {{ ref('stg_workout') }}
GROUP BY instructor