{{ 
  config(
    materialized = 'table',
    unique_key   = ['workout_id'],
 )
}}

{% if target.name == 'prod' %}
  {{ config(
      partition_by = {"field": "workout_date", "data_type": "date"},
      cluster_by   = ['fitness_discipline', 'instructor'],
      grants       = {'roles/bigquery.dataViewer': ['user:pamela@schemanest.com']}
  ) }}
{% endif %}

SELECT workout_id,
       workout_timestamp,
       DATE(workout_timestamp) AS workout_date,
       fitness_discipline,
       instructor,
       length_minutes,
       total_output,
       ROUND((total_output / NULLIF(length_minutes, 0)), 2) AS output_per_min,
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
{% if target.name != 'prod' and var('apply_dev_date_filter', true) %}
    WHERE DATE(workout_timestamp) >= date_sub(current_date(), interval 1 year)
{% endif %}