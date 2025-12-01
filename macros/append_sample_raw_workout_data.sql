{% macro append_sample_raw_workout_data() %}
  {% if target.type != 'bigquery' %}
    {% do exceptions.warn("append_sample_raw_workout_data: This macro is written for BigQuery. Skipping on target.type=" ~ target.type) %}
    {% do return(None) %}
  {% endif %}

  {{ log("Appending sample rows into `" ~ target.project ~ "`.sources.raw_workout_data", info=True) }}

  {% call statement('append_sample_raw_workout_data', fetch_result=False) %}
    INSERT INTO `{{ target.project }}`.sources.raw_workout_data (
      workout_id,
      workout_timestamp,
      length_minutes,
      class_title,
      total_output,
      avg_watts,
      distance_km,
      calories_burned,
      avg_heartrate_bpm,
      fitness_discipline,
      instructor,
      INSERT_TIMESTAMP,
      ROW_COUNT
    )
    VALUES
      ('w5', TIMESTAMP('2025-10-05 06:30:00+00'),
       30, 'Morning Power Ride', 410.0,
       230.0, 15.1, 420,
       150, 'Cycling', 'Pam dbt',
       CURRENT_TIMESTAMP(), 1),

      ('w6', TIMESTAMP('2025-10-06 19:00:00+00'),
       60, 'Evening Endurance Run', 600.0,
       190.0, 10.2, 650,
       158, 'Treadmill', 'Jen dbt',
       CURRENT_TIMESTAMP(), 1)
  {% endcall %}

  {{ log("Append complete for `" ~ target.project ~ "`.sources.raw_workout_data", info=True) }}
{% endmacro %}