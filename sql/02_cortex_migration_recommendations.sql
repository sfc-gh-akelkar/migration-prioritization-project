-- =============================================================================
-- CORTEX-POWERED MIGRATION RECOMMENDATIONS
-- Uses AI to analyze usage metrics and generate prioritized migration waves
-- =============================================================================

WITH top_models AS (
    SELECT 
        model_name,
        database_name || '.' || schema_name AS location,
        total_queries_90d,
        distinct_users_90d,
        downstream_object_count,
        days_since_last_use,
        criticality_score
    FROM {{database}}.{{schema}}.MODEL_USAGE_METRICS
    ORDER BY criticality_score DESC
    LIMIT 50
),
models_json AS (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS data FROM top_models
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'You are a data migration consultant. Analyze these models and create a migration plan.

For each model, assign:
- Migration Wave (1 = highest priority, migrate first)
- Risk Level (High/Medium/Low)
- Reasoning (1 sentence)

Consider these factors:
- Higher query counts and user counts = higher priority
- Models with downstream dependencies should migrate BEFORE their dependents
- Models not used in 60+ days may be candidates for deprecation review

Models data:
' || data::STRING || '

Return a markdown table with columns: Model | Wave | Risk | Reasoning'
) AS migration_plan
FROM models_json;
