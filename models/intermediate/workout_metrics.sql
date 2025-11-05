{{ config(materialized='ephemeral') }}

WITH src AS (
    SELECT workout_id,
           workout_timestamp,
           length_minutes,
           total_output,
           avg_watts,
           distance_km,
           calories_burned,
           avg_heartrate_bpm,
           fitness_discipline,
           instructor
    FROM {{ ref('stg_workout') }}
)

SELECT workout_id,
       workout_timestamp,
       DATE(workout_timestamp) AS workout_date, --TD make db agnostic
       length_minutes,
       total_output,
       avg_watts,
       distance_km,
       calories_burned,
       avg_heartrate_bpm,
       fitness_discipline,
       instructor,
       CASE
           WHEN nullif(length_minutes, 0) IS NULL THEN NULL
           ELSE distance_km / (length_minutes / 60.0)
       END AS avg_speed_kmh,
       CASE
           WHEN length_minutes IS NULL OR length_minutes = 0 THEN NULL
           ELSE total_output / length_minutes
       END AS output_per_min,
       CASE
           WHEN avg_heartrate_bpm IS NULL THEN 'unknown'
           WHEN avg_heartrate_bpm < 110  THEN 'easy'
           WHEN avg_heartrate_bpm < 140  THEN 'moderate'
           WHEN avg_heartrate_bpm < 165  THEN 'hard'
           ELSE 'max'
       END AS intensity_band
FROM src