-- macros/bootstrap_sources_raw_workout_data.sql
{% macro bootstrap_sources_raw_workout_data() %}
  {% if target.type != 'bigquery' %}
    {% do exceptions.warn("bootstrap_sources_raw_workout_data: This macro is written for BigQuery. Skipping on target.type=" ~ target.type) %}
    {% do return(None) %}
  {% endif %}

  {#-- 1) Ensure dataset exists --#}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS `"
      ~ target.project ~ "`.sources") %}

  {#-- 2) Ensure table exists --#}
  {% do run_query("
    CREATE TABLE IF NOT EXISTS `"
      ~ target.project ~ "`.sources.raw_workout_data (
        workout_id STRING,
        workout_timestamp TIMESTAMP,
        length_minutes INT64,
        class_title STRING,
        total_output FLOAT64,
        avg_watts FLOAT64,
        distance_km FLOAT64,
        calories_burned INT64,
        avg_heartrate_bpm INT64,
        fitness_discipline STRING,
        instructor STRING,
        INSERT_TIMESTAMP TIMESTAMP,
        ROW_COUNT INT64,
      )
  ") %}

  {#-- 3) Seed starter rows only if table is empty --#}
  {% do run_query("
    INSERT INTO `"
      ~ target.project ~ "`.sources.raw_workout_data
      (workout_id, workout_timestamp, length_minutes, class_title, total_output, avg_watts, distance_km, calories_burned, avg_heartrate_bpm, fitness_discipline, instructor, INSERT_TIMESTAMP, ROW_COUNT)
    SELECT * FROM UNNEST([
      STRUCT('w1' AS workout_id, TIMESTAMP('2025-10-01 09:00:00+00') AS workout_timestamp,
             30 AS length_minutes, 'HIIT Ride' AS class_title, 320.5 AS total_output,
             210.0 AS avg_watts, 14.2 AS distance_km, 350 AS calories_burned,
             145 AS avg_heartrate_bpm, 'Cycling' AS fitness_discipline, 'John dbt' AS instructor,
             TIMESTAMP('2025-11-05 18:30:00+00') AS INSERT_TIMESTAMP,
             3 AS ROW_COUNT),
      STRUCT('w2', TIMESTAMP('2025-10-02 18:30:00+00'),
             45, 'Endurance Run', 500.0,
             185.0, 8.5, 520,
             152, 'Treadmill', 'Jen dbt', TIMESTAMP('2025-11-05 18:30:00+00'), 3),
      STRUCT('w3', TIMESTAMP('2025-10-03 07:15:00+00'),
             20, 'Yoga Flow', 120.0,
             NULL, 0.0, 80,
             98, 'Yoga', 'Tim dbt', TIMESTAMP('2025-11-05 18:30:00+00'), 3)
    ])
    WHERE NOT EXISTS (
      SELECT 1
      FROM `"
        ~ target.project ~ "`.sources.raw_workout_data
      LIMIT 1
    )
  ") %}

  {% do log("Bootstrap complete for `"
      ~ target.project ~ "`.sources.raw_workout_data", info=True) %}
{% endmacro %}