{{
  config(
      materialized = 'incremental'
  )
}}

WITH empty_table AS (
    SELECT
        STRING(null) AS invocation_id
        , STRING(null) AS query_id
        , STRING(null) AS unique_id
        , STRING(null) AS run_type
        , STRING(null) AS database_name
        , STRING(null) AS schema_name
        , STRING(null) AS table_name
        , STRING(null) AS tags
        , CURRENT_TIMESTAMP() AS started_at
        , CURRENT_TIMESTAMP() AS completed_at
        , STRING(null) AS status
        , STRING(null) AS message
        , FLOAT(null) AS execution_time
        , INT(null) AS rows_affected
        , STRING(null) AS dbt_vars
        , CURRENT_TIMESTAMP() AS loading_time
)

SELECT *
FROM empty_table
WHERE 1 = 0
