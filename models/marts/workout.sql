{% set env_name = env_var('DBT_ENV_TIER', 'local') %}

{{ 
  config(
    materialized = 'table',
    unique_key   = ['workout_id'],
 )
}}

{% if env_name == 'prod' %}
  {{ config(
      partition_by = {"field": "workout_date", "data_type": "date"},
      cluster_by   = ['fitness_discipline', 'instructor'],
      grants       = {'roles/bigquery.dataViewer': ['user:pamela@schemanest.com']}
  ) }}
{% endif %}

{{ log("Building " ~ this.schema ~ "." ~ this.name ~ " in target " ~ env_name, info=True) }}

SELECT workout_id,
       '{{ target.name }}' AS target_name,
       '{{ env_name }}' as dbt_environment_name,
       '{{ target.database }}' AS target_database,
       '{{ target.schema}}' AS target_schema,
       '{{ target.profile_name}}' AS target_profile_name,
       TIMESTAMP('{{ run_started_at }}') AS dbt_run_started_at,
       '{{ invocation_id }}' AS dbt_invocation_id,
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
{% if env_name != 'prod' and var('apply_dev_date_filter', true) %}
    WHERE DATE(workout_timestamp) >= date_sub(current_date(), interval 1 year)
{% endif %}

{% if flags.FULL_REFRESH %}
  {{ log("Running in FULL_REFRESH mode, doing extra cleanup", info=True) }}
{% endif %}