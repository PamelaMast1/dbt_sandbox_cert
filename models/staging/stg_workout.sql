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
        r.instructor,
        r.INSERT_TIMESTAMP,
        r.ROW_COUNT INT64,
        row_number() OVER (PARTITION BY workout_id
                           ORDER BY INSERT_TIMESTAMP DESC, workout_timestamp DESC
        ) as rn
FROM {{ source('raw', 'raw_workout_data') }} AS r
QUALIFY rn = 1
