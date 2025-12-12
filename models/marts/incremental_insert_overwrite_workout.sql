{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    partition_by = {
      'field': 'workout_date',
      'data_type': 'date'
    },
    cluster_by = ['fitness_discipline', 'instructor'],
    on_schema_change     = 'append_new_columns'
) }}

 -- limit how much of the target table BQ scans
   --  incremental_predicates = [ 
     -- "DBT_INTERNAL_DEST.workout_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"

WITH test_data AS (

     -- same id new date and change
     SELECT 'w1' AS workout_id,
            DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY) AS workout_timestamp,
            DATE(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)) AS workout_date,
            'Running' AS fitness_discipline,
            'toby dbt' AS instructor,
             500 AS length_minutes
),
base AS (

    SELECT workout_id,
           workout_timestamp,
           workout_date,
           fitness_discipline,
           instructor,
           length_minutes
    FROM {{ ref('workout_metrics') }}

    {% if is_incremental() %}
      -- STRAGGLER WINDOW:
      -- Rebuild only the last 7 days of partitions, to catch late-arriving workouts
      WHERE workout_date >= (
        SELECT
          DATE_SUB(
            COALESCE(max(workout_date), DATE('1970-01-01')),
            interval 7 DAY
          )
        FROM {{ this }}
      )
    {% endif %}
)

SELECT *
FROM base

UNION ALL

SELECT *
FROM test_data
