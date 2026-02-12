-- ============================================================================
-- Filename: 03_semantic_view.sql
-- Description: Semantic View for Cortex Analyst natural language queries
-- ============================================================================

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
