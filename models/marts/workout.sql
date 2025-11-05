{{
  config(
    unique_key=['workout_id']
  )
}}

SELECT  r.workout_id,
        r.workout_timestamp,
        r.length_minutes,
        r.class_title,
        r.total_output,
        r.avg_watts,
        r.distance_km,
        r.calories_burned,
        r.avg_heartrate_bpm,
        r.fitness_discipline,
        r.instructor
FROM {{ ref('stg_workout') }} AS r