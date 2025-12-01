{{ config(materialized='view') }}

SELECT id,
       field1,
       field2,
       {{ dbt_utils.generate_surrogate_key(['field1', 'field2']) }} as sk
from {{ ref('surrogate_overrides_demo') }}