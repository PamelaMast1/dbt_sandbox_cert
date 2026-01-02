{{ config(materialized='table') }}

SELECT workout_id,
       workout_timestamp,
       workout_date,
       fitness_discipline,
       instructor,
       length_minutes,
       total_output,
       distance_km,
       calories_burned,
       avg_heartrate_bpm,
       avg_speed_kmh,
       output_per_min,
       intensity_band
FROM {{ ref('workout_metrics') }}