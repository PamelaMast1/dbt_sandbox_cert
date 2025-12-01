{{ config(materialized='view') }}

WITH params AS (
  SELECT DATE('2020-01-01') AS start_date,
         DATE_ADD(CURRENT_DATE(), INTERVAL 365 DAY) AS end_date
),

spine AS (
  SELECT day AS date_day
  FROM params,
  UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS day
)

SELECT date_day
FROM spine