{{ config(
    materialized = 'incremental'
) }}

WITH test_data AS (

     -- same id new date and change
     SELECT 'w1' AS workout_id,
            CURRENT_TIMESTAMP() AS workout_timestamp,
            DATE(CURRENT_TIMESTAMP()) AS workout_date,
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
      -- Only grab rows newer than the latest already loaded
      WHERE workout_timestamp > (
        SELECT MAX(workout_timestamp) FROM {{ this }}
      )
    {% endif %}

)

SELECT *
FROM base

UNION ALL

SELECT *
FROM test_data


-- SELECT *
-- FROM base
--         ) as DBT_INTERNAL_SOURCE
--         on (FALSE)

--     when not matched then insert
--         (cols)
--     values
--         (cols`)