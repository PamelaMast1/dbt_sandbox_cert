{{ config(
    materialized = 'view'
) }}

SELECT DATE(workout_timestamp) AS workout_date,
       fitness_discipline,
       instructor,
       COUNT(*) AS workout_count,
       SUM(calories_burned) AS total_calories,
       AVG(avg_heartrate_bpm) AS avg_heartrate_bpm,
       AVG(total_output) AS avg_total_output
FROM {{ ref('workout') }}
GROUP BY workout_date,
         fitness_discipline,
         instructor