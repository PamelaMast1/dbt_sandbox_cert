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


-- 0 - Create the temp table
--   create or replace table `dbt-sandbox-dev`.`dbt_pmasters_marts`.incremental_insert_overwrite_workout__dbt_tmp
      
--     partition by workout_date
--     cluster by fitness_discipline, instructor
     
     --GRAB QUERY
  

-- -- generated script to merge partitions into `dbt-sandbox-dev`.`dbt_pmasters_marts`.incremental_insert_overwrite_workout
--       declare dbt_partitions_for_replacement array<date>;

      
--         -- 1. temp table already exists, we used it to check for schema changes
--       -- 2. define partitions to update
--       set (dbt_partitions_for_replacement) = (
--           select as struct
--               -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
--               array_agg(distinct date(workout_date) IGNORE NULLS)
--           from `dbt-sandbox-dev`.`dbt_pmasters_marts`.incremental_insert_overwrite_workout__dbt_tmp
--       );

--       -- 3. run the merge statement
      

--     merge into `dbt-sandbox-dev`.`dbt_pmasters_marts`.incremental_insert_overwrite_workout as DBT_INTERNAL_DEST
--         using (
--         select
--         * from `dbt-sandbox-dev`.`dbt_pmasters_marts`.incremental_insert_overwrite_workout__dbt_tmp
--       ) as DBT_INTERNAL_SOURCE
--         on FALSE

--     when not matched by source
--          and date(DBT_INTERNAL_DEST.workout_date) in unnest(dbt_partitions_for_replacement) 
--         then delete

--     when not matched then insert
--         (`workout_id`, `workout_timestamp`, `workout_date`, `fitness_discipline`, `instructor`, `length_minutes`)
--     values
--         (`workout_id`, `workout_timestamp`, `workout_date`, `fitness_discipline`, `instructor`, `length_minutes`)

--       -- 4. clean up the temp table
--       drop table if exists `dbt-sandbox-dev`.`dbt_pmasters_marts`.incremental_insert_overwrite_workout__dbt_tmp
