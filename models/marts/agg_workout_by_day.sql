{{ config(materialized='table') }}

SELECT DATE(workout_timestamp) AS workout_date,
       COUNT(*) AS workout_count,
       SUM(calories_burned) AS total_calories,
       SUM(total_output) AS total_output,
       AVG(avg_heartrate_bpm) AS avg_heartrate_bpm
FROM {{ ref('int_workout') }}
GROUP BY DATE(workout_timestamp)