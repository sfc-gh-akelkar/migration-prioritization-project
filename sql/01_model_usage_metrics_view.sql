-- =============================================================================
-- MODEL USAGE METRICS VIEW
-- Aggregates usage patterns and dependencies for migration prioritization
-- =============================================================================

CREATE OR REPLACE VIEW {{database}}.{{schema}}.MODEL_USAGE_METRICS AS
WITH object_access AS (
    SELECT
        obj.value:objectDomain::STRING AS object_type,
        SPLIT_PART(obj.value:objectName::STRING, '.', 1) AS database_name,
        SPLIT_PART(obj.value:objectName::STRING, '.', 2) AS schema_name,
        SPLIT_PART(obj.value:objectName::STRING, '.', 3) AS object_name,
        ah.USER_NAME,
        ah.QUERY_ID,
        ah.QUERY_START_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
         LATERAL FLATTEN(input => ah.BASE_OBJECTS_ACCESSED) obj
    WHERE ah.QUERY_START_TIME >= DATEADD('day', -90, CURRENT_TIMESTAMP())
      AND obj.value:objectDomain::STRING IN ('Table', 'View', 'Materialized View')
      AND ah.USER_NAME NOT IN ('SYSTEM', 'SNOWFLAKE')
      AND ah.USER_NAME NOT LIKE 'STPLAT%'
      AND ah.USER_NAME NOT LIKE '%_SERVICE'
      AND ah.USER_NAME NOT LIKE 'SYS_%'
),

query_metrics AS (
    SELECT
        oa.database_name,
        oa.schema_name,
        oa.object_name,
        oa.object_type,
        COUNT(DISTINCT oa.QUERY_ID) AS total_queries_90d,
        COUNT(DISTINCT oa.USER_NAME) AS distinct_users_90d,
        LISTAGG(DISTINCT oa.USER_NAME, ', ') WITHIN GROUP (ORDER BY oa.USER_NAME) AS users_list,
        MAX(oa.QUERY_START_TIME) AS last_used_at,
        ROUND(AVG(qh.TOTAL_ELAPSED_TIME), 2) AS avg_execution_time_ms,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qh.TOTAL_ELAPSED_TIME), 2) AS p95_execution_time_ms,
        ROUND(AVG(qh.BYTES_SCANNED), 0) AS avg_bytes_scanned,
        ROUND(AVG(qh.ROWS_PRODUCED), 0) AS avg_rows_returned,
        ROUND(SUM(CASE WHEN qh.EXECUTION_STATUS != 'SUCCESS' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 4) AS error_rate
    FROM object_access oa
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        ON oa.QUERY_ID = qh.QUERY_ID
    WHERE oa.object_name IS NOT NULL AND oa.object_name != ''
    GROUP BY 1, 2, 3, 4
),

dependency_metrics AS (
    SELECT
        REFERENCED_DATABASE AS database_name,
        REFERENCED_SCHEMA AS schema_name,
        REFERENCED_OBJECT_NAME AS object_name,
        COUNT(DISTINCT REFERENCING_OBJECT_NAME) AS downstream_object_count,
        COUNT(DISTINCT CASE WHEN REFERENCING_OBJECT_DOMAIN = 'VIEW' THEN REFERENCING_OBJECT_NAME END) AS downstream_view_count,
        COUNT(DISTINCT CASE WHEN REFERENCING_OBJECT_DOMAIN = 'TABLE' THEN REFERENCING_OBJECT_NAME END) AS downstream_table_count,
        LISTAGG(DISTINCT REFERENCING_OBJECT_NAME, ', ') WITHIN GROUP (ORDER BY REFERENCING_OBJECT_NAME) AS downstream_objects_list
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE REFERENCED_OBJECT_NAME IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    q.database_name,
    q.schema_name,
    q.object_name AS model_name,
    q.object_type,
    q.total_queries_90d,
    q.distinct_users_90d,
    q.users_list,
    q.last_used_at,
    DATEDIFF('day', q.last_used_at, CURRENT_TIMESTAMP()) AS days_since_last_use,
    q.avg_execution_time_ms,
    q.p95_execution_time_ms,
    q.avg_bytes_scanned,
    q.error_rate,
    COALESCE(d.downstream_object_count, 0) AS downstream_object_count,
    COALESCE(d.downstream_view_count, 0) AS downstream_view_count,
    d.downstream_objects_list,
    ROUND(
        (q.total_queries_90d * 0.5) +
        (q.distinct_users_90d * 20) +
        (COALESCE(d.downstream_object_count, 0) * 25),
    2) AS criticality_score
FROM query_metrics q
LEFT JOIN dependency_metrics d
    ON UPPER(q.database_name) = UPPER(d.database_name)
    AND UPPER(q.schema_name) = UPPER(d.schema_name)
    AND UPPER(q.object_name) = UPPER(d.object_name)
WHERE q.total_queries_90d > 0;
