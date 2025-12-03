{# 
  Workout intensity + volume summary by week and fitness discipline.

  This is an analysis file:
  - dbt will compile it (so ref() works)
  - but it won't create a table/view in the warehouse.
#}

{% set lookback_days = var('analysis_lookback_days', 90) %}

WITH base AS 
    ( 
        SELECT workout_id,
        workout_timestamp,
        DATE(workout_timestamp) AS workout_date,
        fitness_discipline,
        instructor,
        length_minutes,
        total_output,
        output_per_min,
        distance_km,
        calories_burned,
        avg_heartrate_bpm,
        avg_speed_kmh,
        intensity_band
    FROM {{ ref('workout_metrics') }}
    WHERE workout_timestamp >= timestamp_sub(current_timestamp(), interval {{ lookback_days }} DAY)
),

by_week AS (
    SELECT  date_trunc(workout_date, week(monday)) AS week_start_date,
            fitness_discipline,
            count(*) AS workout_count,
            sum(length_minutes) AS total_minutes,
            sum(total_output) AS total_output,
            avg(output_per_min) AS avg_output_per_min,
            avg(avg_heartrate_bpm) AS avg_heartrate_bpm,
            avg(avg_speed_kmh) AS avg_speed_kmh
    FROM base
    GROUP BY 1, 2
),

ranked AS (
    SELECT *,
        dense_rank() OVER (PARTITION BY week_start_date ORDER BY total_output DESC) AS discipline_rank_by_output
    FROM by_week
)

SELECT week_start_date,
       fitness_discipline,
       workout_count,
       total_minutes,
       total_output,
       avg_output_per_min,
       avg_heartrate_bpm,
       avg_speed_kmh,
       discipline_rank_by_output
FROM ranked
ORDER BY week_start_date DESC, total_output DESC