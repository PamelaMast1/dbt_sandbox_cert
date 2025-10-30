SELECT *
    FROM {{ source('bronze', 'raw_workout_data') }} AS n 