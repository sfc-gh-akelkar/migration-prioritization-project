-- =============================================================================
-- SAMPLE ANALYSIS QUERIES
-- Common queries for migration planning and analysis
-- =============================================================================

-- High-risk models: high usage + many dependencies
SELECT *
FROM {{database}}.{{schema}}.MODEL_USAGE_METRICS
WHERE total_queries_90d > 100
  AND downstream_object_count > 3
ORDER BY criticality_score DESC;

-- Stale models: no recent usage but have dependencies (cleanup candidates)
SELECT *
FROM {{database}}.{{schema}}.MODEL_USAGE_METRICS
WHERE days_since_last_use > 60
  AND downstream_object_count > 0
ORDER BY downstream_object_count DESC;

-- Models by user impact
SELECT 
    database_name,
    schema_name,
    model_name,
    distinct_users_90d,
    users_list,
    total_queries_90d
FROM {{database}}.{{schema}}.MODEL_USAGE_METRICS
WHERE distinct_users_90d > 1
ORDER BY distinct_users_90d DESC, total_queries_90d DESC;

-- Performance hotspots: slow queries that are frequently used
SELECT 
    database_name,
    schema_name,
    model_name,
    total_queries_90d,
    avg_execution_time_ms,
    p95_execution_time_ms,
    avg_bytes_scanned
FROM {{database}}.{{schema}}.MODEL_USAGE_METRICS
WHERE avg_execution_time_ms > 1000
  AND total_queries_90d > 50
ORDER BY avg_execution_time_ms DESC;

-- Dependency chain analysis: models with most downstream impact
SELECT 
    database_name,
    schema_name,
    model_name,
    downstream_object_count,
    downstream_view_count,
    downstream_objects_list
FROM {{database}}.{{schema}}.MODEL_USAGE_METRICS
WHERE downstream_object_count > 0
ORDER BY downstream_object_count DESC
LIMIT 20;
