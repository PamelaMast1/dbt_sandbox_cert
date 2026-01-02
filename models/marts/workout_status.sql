SELECT 1 AS order_id, cast('pending'   AS {{ dbt.type_string() }}) AS STATUS UNION ALL
SELECT 2 AS order_id, cast('completed' AS {{ dbt.type_string() }}) AS STATUS UNION ALL
SELECT 3 AS order_id, cast('cancelled' AS {{ dbt.type_string() }}) AS STATUS UNION ALL
SELECT 4 AS order_id, cast(1           AS {{ dbt.type_string() }}) AS STATUS UNION ALL
SELECT 5 AS order_id, cast(true        AS {{ dbt.type_string() }}) AS STATUS UNION ALL
SELECT 6 AS order_id, cast(null        AS {{ dbt.type_string() }}) AS STATUS