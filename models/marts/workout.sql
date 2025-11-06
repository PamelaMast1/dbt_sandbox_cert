{{
  config(
    unique_key=['workout_id']
  )
}}

SELECT workout_id,
       workout_timestamp,
       DATE(workout_timestamp) AS workout_date,
       fitness_discipline,
       instructor,
       length_minutes,
       total_output,
       ROUND((total_output / NULLIF(length_minutes, 0)), 2)AS output_per_min,
       distance_km,
       calories_burned,
       avg_heartrate_bpm,
       ROUND((distance_km / NULLIF(length_minutes / 60.0, 0)), 2) AS avg_speed_kmh,
       CASE
         WHEN (total_output / NULLIF(length_minutes, 0)) IS NULL THEN 'n/a'
         WHEN (total_output / NULLIF(length_minutes, 0)) < 6 THEN 'Low'
         WHEN (total_output / NULLIF(length_minutes, 0)) <= 10 THEN 'Moderate'
         ELSE 'High'
       END AS intensity_band,
       NULLIF(avg_watts, 0) AS avg_watts
FROM {{ ref('stg_workout') }}