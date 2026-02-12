-- ============================================================================
-- Filename: deploy.sql
-- Description: Master deployment script for Migration Prioritization solution
--
-- Prerequisites: 
--   - Snowflake Enterprise Edition (required for ACCESS_HISTORY)
--   - ACCOUNTADMIN or role with:
--     - Access to SNOWFLAKE.ACCOUNT_USAGE schema
--     - SNOWFLAKE.CORTEX_USER database role (for AISQL functions)
--     - CREATE DATABASE, CREATE SCHEMA privileges
--
-- Deployment Order:
--   1. Create database and schema
--   2. Deploy MODEL_USAGE_METRICS table
--   3. Deploy MIGRATION_SCORING view and MIGRATION_PLAN table
--   4. Deploy MIGRATION_PLANNING_SEM semantic view
--   5. Deploy Cortex Agent for Snowflake Intelligence
--
-- Usage:
--   1. Update CONFIGURATION section below with your values
--   2. Run: snow sql -f sql/deploy.sql
--   OR copy/paste into a Snowsight worksheet and execute
--
-- Estimated Runtime: 5-15 minutes (depending on model count)
-- ============================================================================

-- =============================================================================
-- CONFIGURATION - UPDATE THESE VALUES FOR YOUR ENVIRONMENT
-- =============================================================================
-- Target database and schema for migration planning objects
SET TARGET_DATABASE = 'MIGRATION_PLANNING';
SET TARGET_SCHEMA = 'ANALYTICS';

-- Warehouse for Cortex Agent execution (must exist in your account)
SET AGENT_WAREHOUSE = 'COMPUTE_WH';

-- Lookback window for usage analysis (default: 90 days)
SET LOOKBACK_DAYS = 90;

-- =============================================================================
-- STEP 0: PRE-FLIGHT CHECKS
-- =============================================================================

SELECT '[INFO] Starting Migration Prioritization deployment...' AS status;

-- Verify Enterprise Edition (required for ACCESS_HISTORY)
SELECT 
    CASE 
        WHEN CURRENT_ACCOUNT_NAME() IS NOT NULL 
        THEN '[PASS] Connected to Snowflake account: ' || CURRENT_ACCOUNT_NAME()
        ELSE '[FAIL] Not connected to Snowflake'
    END AS preflight_check;

-- Verify CORTEX_USER role access
SELECT '[INFO] Verifying Cortex access...' AS status;


-- =============================================================================
-- STEP 1: CREATE DATABASE AND SCHEMA
-- =============================================================================

SELECT '[STEP 1/4] Creating database and schema...' AS status;

CREATE DATABASE IF NOT EXISTS MIGRATION_PLANNING
    COMMENT = 'Migration prioritization analysis using Cortex AI';

CREATE SCHEMA IF NOT EXISTS MIGRATION_PLANNING.ANALYTICS
    COMMENT = 'Analytics views and tables for migration planning';

SELECT '[PASS] Database MIGRATION_PLANNING.ANALYTICS created' AS status;


-- =============================================================================
-- STEP 2: CREATE MODEL USAGE METRICS VIEW
-- =============================================================================

SELECT '[STEP 2/4] Creating MODEL_USAGE_METRICS table...' AS status;

CREATE OR REPLACE TABLE MIGRATION_PLANNING.ANALYTICS.MODEL_USAGE_METRICS AS

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
        ROUND(
            SUM(CASE WHEN qh.EXECUTION_STATUS != 'SUCCESS' THEN 1 ELSE 0 END)::FLOAT 
            / NULLIF(COUNT(*), 0), 
        4) AS error_rate
    FROM object_access oa
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        ON oa.QUERY_ID = qh.QUERY_ID
    WHERE oa.object_name IS NOT NULL 
      AND oa.object_name != ''
    GROUP BY 1, 2, 3, 4
),

dependency_metrics AS (
    SELECT
        REFERENCED_DATABASE AS database_name,
        REFERENCED_SCHEMA AS schema_name,
        REFERENCED_OBJECT_NAME AS object_name,
        COUNT(DISTINCT REFERENCING_OBJECT_NAME) AS downstream_object_count,
        COUNT(DISTINCT CASE 
            WHEN REFERENCING_OBJECT_DOMAIN = 'VIEW' THEN REFERENCING_OBJECT_NAME 
        END) AS downstream_view_count,
        COUNT(DISTINCT CASE 
            WHEN REFERENCING_OBJECT_DOMAIN = 'TABLE' THEN REFERENCING_OBJECT_NAME 
        END) AS downstream_table_count,
        LISTAGG(DISTINCT REFERENCING_OBJECT_NAME, ', ') 
            WITHIN GROUP (ORDER BY REFERENCING_OBJECT_NAME) AS downstream_objects_list
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
    q.avg_rows_returned,
    q.error_rate,
    COALESCE(d.downstream_object_count, 0) AS downstream_object_count,
    COALESCE(d.downstream_view_count, 0) AS downstream_view_count,
    COALESCE(d.downstream_table_count, 0) AS downstream_table_count,
    d.downstream_objects_list,
    ROUND(
        (q.total_queries_90d * 0.4) +
        (q.distinct_users_90d * 15) +
        (COALESCE(d.downstream_object_count, 0) * 20) +
        (CASE WHEN q.error_rate > 0.05 THEN 10 ELSE 0 END),
    2) AS criticality_score
FROM query_metrics q
LEFT JOIN dependency_metrics d
    ON UPPER(q.database_name) = UPPER(d.database_name)
    AND UPPER(q.schema_name) = UPPER(d.schema_name)
    AND UPPER(q.object_name) = UPPER(d.object_name)
WHERE q.total_queries_90d > 0;

-- Validate Step 2
SELECT 
    '[PASS] MODEL_USAGE_METRICS table created with ' || COUNT(*) || ' models' AS status
FROM MIGRATION_PLANNING.ANALYTICS.MODEL_USAGE_METRICS;


-- =============================================================================
-- STEP 3: CREATE MIGRATION SCORING WITH AISQL
-- =============================================================================

SELECT '[STEP 3/4] Creating MIGRATION_SCORING view and MIGRATION_PLAN table...' AS status;
SELECT '[INFO] This step uses AI_CLASSIFY and AI_COMPLETE - may take several minutes...' AS status;

-- Step 3a: Create scoring view (real-time classification)
CREATE OR REPLACE VIEW MIGRATION_PLANNING.ANALYTICS.MIGRATION_SCORING AS
SELECT
    m.database_name,
    m.schema_name,
    m.model_name,
    m.object_type,
    m.total_queries_90d,
    m.distinct_users_90d,
    m.users_list,
    m.downstream_object_count,
    m.downstream_objects_list,
    m.avg_execution_time_ms,
    m.p95_execution_time_ms,
    m.avg_bytes_scanned,
    m.error_rate,
    m.days_since_last_use,
    m.criticality_score,
    
    -- AI_CLASSIFY for impact bucketing (returns {"labels":["X"]} format)
    PARSE_JSON(AI_CLASSIFY(
        'Model: ' || m.model_name || 
        ' | Queries (90d): ' || m.total_queries_90d || 
        ' | Users: ' || m.distinct_users_90d || 
        ' | Downstream deps: ' || m.downstream_object_count,
        ARRAY_CONSTRUCT('HIGH', 'MEDIUM', 'LOW')
    )::STRING):labels[0]::STRING AS impact_level,
    
    -- AI_CLASSIFY for risk bucketing
    PARSE_JSON(AI_CLASSIFY(
        'Model: ' || m.model_name || 
        ' | Downstream objects: ' || m.downstream_object_count || 
        ' | Avg exec time ms: ' || COALESCE(m.avg_execution_time_ms, 0) || 
        ' | Error rate: ' || COALESCE(m.error_rate, 0) ||
        ' | Days since last use: ' || m.days_since_last_use,
        ARRAY_CONSTRUCT('HIGH', 'MEDIUM', 'LOW')
    )::STRING):labels[0]::STRING AS risk_level,
    
    -- Rules-based migration wave
    CASE
        WHEN m.days_since_last_use > 90 THEN 'DEPRECATE'
        WHEN m.total_queries_90d > 100 AND m.downstream_object_count <= 3 THEN 'Wave 1'
        WHEN m.total_queries_90d > 100 AND m.downstream_object_count > 3 THEN 'Wave 2'
        WHEN m.total_queries_90d > 20 THEN 'Wave 2'
        ELSE 'Wave 3'
    END AS migration_wave

FROM MIGRATION_PLANNING.ANALYTICS.MODEL_USAGE_METRICS m;

SELECT '[PASS] MIGRATION_SCORING view created' AS status;

-- Step 3b: Materialize with AI-generated rationale
SELECT '[INFO] Generating AI rationales for migration plan (this may take a few minutes)...' AS status;

CREATE OR REPLACE TABLE MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN AS
SELECT
    s.database_name,
    s.schema_name,
    s.model_name,
    s.object_type,
    s.total_queries_90d,
    s.distinct_users_90d,
    s.users_list,
    s.downstream_object_count,
    s.downstream_objects_list,
    s.avg_execution_time_ms,
    s.p95_execution_time_ms,
    s.avg_bytes_scanned,
    s.error_rate,
    s.days_since_last_use,
    s.criticality_score,
    s.impact_level,
    s.risk_level,
    s.migration_wave,
    
    -- AI_COMPLETE for detailed rationale
    AI_COMPLETE(
        'claude-3-5-sonnet',
        'You are a data migration consultant. Provide a concise 1-2 sentence rationale for this migration classification.

Model: ' || s.model_name || '
Location: ' || s.database_name || '.' || s.schema_name || '
Metrics:
- Queries (90d): ' || s.total_queries_90d || '
- Distinct users: ' || s.distinct_users_90d || '
- Downstream dependencies: ' || s.downstream_object_count || '
- Avg execution time: ' || COALESCE(s.avg_execution_time_ms, 0) || 'ms
- Days since last use: ' || s.days_since_last_use || '

Classification:
- Impact: ' || s.impact_level || '
- Risk: ' || s.risk_level || '
- Wave: ' || s.migration_wave || '

Explain why this model received this classification and any migration considerations.'
    ) AS migration_rationale,
    
    CURRENT_TIMESTAMP() AS scored_at

FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_SCORING s
WHERE s.total_queries_90d > 0;

-- Validate Step 3
SELECT 
    '[PASS] MIGRATION_PLAN table created' AS status,
    COUNT(*) AS total_models,
    COUNT(CASE WHEN migration_wave = 'Wave 1' THEN 1 END) AS wave_1,
    COUNT(CASE WHEN migration_wave = 'Wave 2' THEN 1 END) AS wave_2,
    COUNT(CASE WHEN migration_wave = 'Wave 3' THEN 1 END) AS wave_3,
    COUNT(CASE WHEN migration_wave = 'DEPRECATE' THEN 1 END) AS deprecate
FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN;


-- =============================================================================
-- STEP 4: CREATE SEMANTIC VIEW FOR CORTEX ANALYST
-- =============================================================================

SELECT '[STEP 4/4] Creating semantic view for Cortex Analyst...' AS status;

CREATE OR REPLACE SEMANTIC VIEW MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLANNING_SEM
  TABLES (
    migration_plan AS MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN 
      PRIMARY KEY (database_name, schema_name, model_name)
  )

  DIMENSIONS (
    migration_plan.database_name AS database_name
      WITH SYNONYMS = ('db', 'database'),
    migration_plan.schema_name AS schema_name
      WITH SYNONYMS = ('schema'),
    migration_plan.model_name AS model_name
      WITH SYNONYMS = ('model', 'table', 'view', 'object', 'asset'),
    migration_plan.object_type AS object_type
      WITH SYNONYMS = ('type', 'kind'),
    migration_plan.impact_level AS impact_level
      WITH SYNONYMS = ('impact', 'business impact', 'importance'),
    migration_plan.risk_level AS risk_level
      WITH SYNONYMS = ('risk', 'complexity', 'migration risk'),
    migration_plan.migration_wave AS migration_wave
      WITH SYNONYMS = ('wave', 'phase', 'priority'),
    migration_plan.users_list AS users_list
      WITH SYNONYMS = ('users', 'who uses', 'consumers'),
    migration_plan.migration_rationale AS migration_rationale
      WITH SYNONYMS = ('reason', 'explanation', 'why', 'rationale')
  )

  METRICS (
    migration_plan.query_count AS SUM(total_queries_90d)
      WITH SYNONYMS = ('queries', 'query count', 'usage'),
    migration_plan.user_count AS SUM(distinct_users_90d)
      WITH SYNONYMS = ('user count', 'how many users'),
    migration_plan.model_count AS COUNT(model_name)
      WITH SYNONYMS = ('models', 'count', 'how many'),
    migration_plan.dependency_count AS SUM(downstream_object_count)
      WITH SYNONYMS = ('dependencies', 'downstream'),
    migration_plan.avg_dependencies AS AVG(downstream_object_count)
      WITH SYNONYMS = ('average dependencies'),
    migration_plan.avg_execution_ms AS AVG(avg_execution_time_ms)
      WITH SYNONYMS = ('execution time', 'performance', 'latency'),
    migration_plan.avg_criticality AS AVG(criticality_score)
      WITH SYNONYMS = ('criticality', 'score'),
    migration_plan.avg_days_since_use AS AVG(days_since_last_use)
      WITH SYNONYMS = ('staleness', 'age', 'last used')
  )

  COMMENT = 'Semantic view for migration planning analysis. Query model usage, impact, risk, and migration wave assignments using natural language.';

SELECT '[PASS] Semantic view MIGRATION_PLANNING_SEM created' AS status;


-- =============================================================================
-- STEP 5: CREATE CORTEX AGENT FOR SNOWFLAKE INTELLIGENCE
-- =============================================================================

SELECT '[STEP 5/5] Creating Cortex Agent for Snowflake Intelligence...' AS status;

-- Ensure SNOWFLAKE_INTELLIGENCE database and schema exist
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

-- Grant access for discoverability
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE PUBLIC;

-- Create the Migration Planning Agent
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.MIGRATION_PLANNING_AGENT
  COMMENT = 'AI assistant for migration planning - analyzes model usage, dependencies, and prioritization'
  PROFILE = '{"display_name": "Migration Planning Assistant", "avatar": "database", "color": "#29B5E8"}'
  FROM SPECIFICATION $
  {
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "You are a migration planning assistant. Use the migration_data tool to answer questions about model usage, dependencies, migration waves, and prioritization. When users ask about which models to migrate first, focus on Wave 1 models. For risk assessment, highlight models with high downstream dependencies.",
      "response": "Provide clear, actionable migration guidance. When showing data, summarize key insights. Always mention the criticality score and migration wave when discussing specific models. Format numbers clearly and round decimals."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "migration_data",
          "description": "Query migration planning data including model usage metrics, dependency counts, impact/risk levels, migration wave assignments, and AI-generated rationales. Use for questions about: which models to migrate first, high-risk models, unused models, dependency analysis, user impact."
        }
      }
    ],
    "tool_resources": {
      "migration_data": {
        "semantic_view": "MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLANNING_SEM",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "COMPUTE_WH"
        },
        "query_timeout": 60
      }
    }
  }
  $;

-- Grant agent access
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.MIGRATION_PLANNING_AGENT TO ROLE PUBLIC;

-- Grant access to the underlying semantic view
GRANT SELECT ON SEMANTIC VIEW MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLANNING_SEM TO ROLE PUBLIC;

SELECT '[PASS] Cortex Agent MIGRATION_PLANNING_AGENT created in Snowflake Intelligence' AS status;


-- =============================================================================
-- DEPLOYMENT COMPLETE
-- =============================================================================

SELECT '[SUCCESS] Migration Prioritization deployment complete!' AS status;

SELECT 
    '=== DEPLOYMENT SUMMARY ===' AS summary,
    NULL AS value
UNION ALL
SELECT 'Database', 'MIGRATION_PLANNING'
UNION ALL
SELECT 'Schema', 'ANALYTICS'
UNION ALL
SELECT 'Objects Created', '5 (1 view, 2 tables, 1 semantic view, 1 agent)'
UNION ALL
SELECT 'Total Models Analyzed', (SELECT COUNT(*)::STRING FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN)
UNION ALL
SELECT 'Wave 1 Models', (SELECT COUNT(*)::STRING FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN WHERE migration_wave = 'Wave 1')
UNION ALL
SELECT 'Wave 2 Models', (SELECT COUNT(*)::STRING FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN WHERE migration_wave = 'Wave 2')
UNION ALL
SELECT 'Wave 3 Models', (SELECT COUNT(*)::STRING FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN WHERE migration_wave = 'Wave 3')
UNION ALL
SELECT 'Deprecation Candidates', (SELECT COUNT(*)::STRING FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN WHERE migration_wave = 'DEPRECATE');

SELECT '
Next Steps:
1. Review migration plan: SELECT * FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN ORDER BY criticality_score DESC;
2. Open Snowflake Intelligence: AI & ML > Snowflake Intelligence
3. Select "Migration Planning Assistant" agent
4. Ask: "Which models should I migrate first?" or "Show me high-risk models"
' AS next_steps;
