-- ============================================================================
-- Filename: 04_cortex_agent.sql
-- Description: Cortex Agent for Snowflake Intelligence
-- 
-- BEFORE RUNNING: Update the warehouse name on line 41 to match your environment
-- ============================================================================

-- Step 1: Ensure SNOWFLAKE_INTELLIGENCE database and schema exist
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

-- Step 2: Grant access for discoverability
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE PUBLIC;

-- Step 3: Create the Migration Planning Agent
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.MIGRATION_PLANNING_AGENT
  COMMENT = 'AI assistant for migration planning - analyzes model usage, dependencies, and prioritization'
  PROFILE = '{"display_name": "Migration Planning Assistant", "avatar": "database", "color": "#29B5E8"}'
  FROM SPECIFICATION $$
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
  $$;

-- Step 4: Grant agent access
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.MIGRATION_PLANNING_AGENT TO ROLE PUBLIC;

-- Step 5: Grant access to the underlying semantic view
GRANT SELECT ON SEMANTIC VIEW MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLANNING_SEM TO ROLE PUBLIC;
