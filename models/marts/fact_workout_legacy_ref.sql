{{ config(materialized='table') }}

SELECT *
FROM {{ ref('fact_workout_legacy') }}