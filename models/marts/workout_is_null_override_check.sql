{{ 
  config(
    materialized = 'table',
    unique_key   = ['workout_id'],
 )
}}


SELECT workout_id,
       CAST(NULL AS STRING) AS fitness_discipline,
       TRUE AS is_deleted 
FROM {{ ref('stg_workout') }}