{{ config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'workout_id',
    on_schema_change    = 'sync_all_columns'  
        
) }}

-- on_schema_change  
-- ignore (default): new columns won’t appear; dropping columns can error.
-- fail: break if schema changes.
-- append_new_columns: add new columns, don’t drop old ones.
-- sync_all_columns: add and remove columns, and update types (BigQuery can require full table scan for type changes).

WITH test_data AS (

     -- same id new date and change
     SELECT 'w1' AS workout_id,
            CURRENT_TIMESTAMP() AS workout_timestamp,
            DATE(CURRENT_TIMESTAMP()) AS workout_date,
            'Running' AS fitness_discipline,
            'toby dbt' AS instructor,
             500 AS length_minutes--,
             --'bbbbb' AS new_column1
),
base AS (

    SELECT workout_id,
           workout_timestamp,
           workout_date,
           fitness_discipline,
           instructor,
           length_minutes--,
           --'bbbbb' AS new_column1
    FROM {{ ref('workout_metrics') }}

    {% if is_incremental() %}
      -- Only reprocess potentially new / changed records
      WHERE workout_timestamp >= (
        SELECT MAX(workout_timestamp) FROM {{ this }}
      )
    {% endif %}

)

SELECT *
FROM base

UNION ALL

SELECT *
FROM test_data

    -- when matched then update set
    -- when not matched then insert
  -- you need to create an override macro to exclude 'ALL' coluns in the set
  -- full refresh for back fill