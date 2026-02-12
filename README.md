# Migration Prioritization with Snowflake Cortex

Automatically prioritize data models for migration using Snowflake's ACCOUNT_USAGE views and Cortex AI functions. This solution analyzes query history, user impact, and object dependencies to generate a prioritized migration plan with AI-powered rationales.

## Overview

This solution implements a 4-step pattern for migration planning:

1. **Model Usage Layer** - Aggregates 90-day usage metrics from ACCOUNT_USAGE views
2. **AI Scoring** - Uses AISQL functions (AI_CLASSIFY, AI_COMPLETE) to classify impact/risk and generate rationales
3. **Semantic View** - Enables natural language queries via Cortex Analyst
4. **Migration Plan** - Materialized table with wave assignments and AI-generated recommendations

## Prerequisites

### Snowflake Requirements

| Requirement | Details |
|-------------|---------|
| **Edition** | Enterprise Edition or higher (required for ACCESS_HISTORY) |
| **Role** | ACCOUNTADMIN or custom role with privileges below |
| **Cortex Access** | SNOWFLAKE.CORTEX_USER database role |

### Required Privileges

```sql
-- Grant access to ACCOUNT_USAGE views
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;

-- Grant Cortex AI functions access
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;

-- Grant object creation privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <your_role>;
-- OR if using existing database:
GRANT CREATE SCHEMA ON DATABASE <your_database> TO ROLE <your_role>;
```

### Tools

- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (recommended) or Snowsight

## Quick Start

### Option 1: Deploy with Snowflake CLI

```bash
# Clone the repository
git clone <repository-url>
cd migration-prioritization-project

# Deploy (takes 5-15 minutes depending on model count)
snow sql -f sql/deploy.sql
```

### Option 2: Deploy via Snowsight

1. Open Snowsight and navigate to **Worksheets**
2. Create a new worksheet
3. Copy the contents of `sql/deploy.sql` into the worksheet
4. Run all statements

## What Gets Created

The deployment creates the following objects in `MIGRATION_PLANNING.ANALYTICS`:

| Object | Type | Description |
|--------|------|-------------|
| `MODEL_USAGE_METRICS` | Table | Snapshot of usage metrics from ACCOUNT_USAGE |
| `MIGRATION_SCORING` | View | AI-classified impact and risk levels |
| `MIGRATION_PLAN` | Table | Materialized plan with AI rationales |
| `MIGRATION_PLANNING_SEM` | Semantic View | Natural language query interface |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SNOWFLAKE.ACCOUNT_USAGE                         │
├──────────────────┬──────────────────┬───────────────────────────────┤
│  ACCESS_HISTORY  │  QUERY_HISTORY   │  OBJECT_DEPENDENCIES          │
│  (object access) │  (performance)   │  (downstream deps)            │
└────────┬─────────┴────────┬─────────┴───────────────┬───────────────┘
         │                  │                         │
         └──────────────────┼─────────────────────────┘
                            ▼
              ┌─────────────────────────────┐
              │    MODEL_USAGE_METRICS      │
              │    (aggregated view)        │
              └─────────────┬───────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │    MIGRATION_SCORING        │
              │  AI_CLASSIFY: impact/risk   │
              │  Rules: wave assignment     │
              └─────────────┬───────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │      MIGRATION_PLAN         │
              │  AI_COMPLETE: rationales    │
              │  (materialized table)       │
              └─────────────┬───────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │   MIGRATION_PLANNING_SEM    │
              │     (semantic view)         │
              │  "Show me Wave 1 models"    │
              └─────────────────────────────┘
```

## How Metrics Are Generated

The `MODEL_USAGE_METRICS` table is the foundation of this solution. It combines data from three Snowflake ACCOUNT_USAGE views to build a comprehensive picture of each object's usage, performance, and dependencies.

### Data Sources

| Source View | What It Provides | Key Fields Used |
|-------------|------------------|-----------------|
| `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` | Object-level access patterns | `BASE_OBJECTS_ACCESSED`, `USER_NAME`, `QUERY_ID`, `QUERY_START_TIME` |
| `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` | Query performance metrics | `TOTAL_ELAPSED_TIME`, `BYTES_SCANNED`, `ROWS_PRODUCED`, `EXECUTION_STATUS` |
| `SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES` | Downstream object relationships | `REFERENCED_*`, `REFERENCING_*` columns |

### Metric Definitions

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `total_queries_90d` | `COUNT(DISTINCT QUERY_ID)` | Number of unique queries that accessed this object in the last 90 days |
| `distinct_users_90d` | `COUNT(DISTINCT USER_NAME)` | Number of unique users who queried this object |
| `users_list` | `LISTAGG(DISTINCT USER_NAME)` | Comma-separated list of users for reference |
| `last_used_at` | `MAX(QUERY_START_TIME)` | Timestamp of most recent access |
| `days_since_last_use` | `DATEDIFF('day', last_used_at, CURRENT_TIMESTAMP())` | Staleness indicator |
| `avg_execution_time_ms` | `AVG(TOTAL_ELAPSED_TIME)` | Average query duration in milliseconds |
| `p95_execution_time_ms` | `PERCENTILE_CONT(0.95)` | 95th percentile execution time (outlier-resistant) |
| `avg_bytes_scanned` | `AVG(BYTES_SCANNED)` | Average data read per query |
| `avg_rows_returned` | `AVG(ROWS_PRODUCED)` | Average result set size |
| `error_rate` | `SUM(failures) / COUNT(*)` | Fraction of queries that failed (0.0 - 1.0) |
| `downstream_object_count` | `COUNT(DISTINCT REFERENCING_OBJECT_NAME)` | Number of views/tables that depend on this object |
| `downstream_view_count` | `COUNT(DISTINCT ... WHERE DOMAIN='VIEW')` | Subset: dependent views only |
| `downstream_table_count` | `COUNT(DISTINCT ... WHERE DOMAIN='TABLE')` | Subset: dependent tables only |
| `downstream_objects_list` | `LISTAGG(DISTINCT REFERENCING_OBJECT_NAME)` | Comma-separated list of dependents |
| `criticality_score` | See formula below | Composite priority score |

### Query Processing Pipeline

```
Step 1: Extract Object Access (object_access CTE)
├── Flatten BASE_OBJECTS_ACCESSED array from ACCESS_HISTORY
├── Parse fully-qualified names into database/schema/object
├── Filter to Tables, Views, Materialized Views only
└── Exclude system users (SYSTEM, SNOWFLAKE, *_SERVICE, SYS_*)

Step 2: Aggregate Query Metrics (query_metrics CTE)
├── Join to QUERY_HISTORY on QUERY_ID
├── Calculate COUNT, AVG, PERCENTILE aggregations
└── Group by database_name, schema_name, object_name, object_type

Step 3: Aggregate Dependencies (dependency_metrics CTE)
├── Query OBJECT_DEPENDENCIES for referenced objects
├── Count distinct downstream objects by type
└── Group by database_name, schema_name, object_name

Step 4: Join and Score (final SELECT)
├── LEFT JOIN query_metrics to dependency_metrics
├── Use UPPER() for case-insensitive matching
├── Calculate criticality_score
└── Filter to objects with total_queries_90d > 0
```

### Filters Applied

The following records are **excluded** from analysis:

| Filter | Reason |
|--------|--------|
| `USER_NAME IN ('SYSTEM', 'SNOWFLAKE')` | Internal Snowflake processes |
| `USER_NAME LIKE '%_SERVICE'` | Service accounts (often automated) |
| `USER_NAME LIKE 'SYS_%'` | System-generated users |
| `object_type NOT IN ('Table', 'View', 'Materialized View')` | Focus on data objects only |
| `total_queries_90d = 0` | Objects with no recent usage |

### Customizing the Metrics Query

You can modify `sql/01_model_usage_metrics.sql` to:

**Include only specific databases:**
```sql
-- In object_access CTE, add:
AND SPLIT_PART(obj.value:objectName::STRING, '.', 1) IN ('PROD_DB', 'ANALYTICS_DB')
```

**Exclude staging/temp schemas:**
```sql
-- In object_access CTE, add:
AND SPLIT_PART(obj.value:objectName::STRING, '.', 2) NOT IN ('STAGING', 'TEMP', 'RAW')
```

**Change the lookback window:**
```sql
-- Change from 90 days to 180 days:
WHERE ah.QUERY_START_TIME >= DATEADD('day', -180, CURRENT_TIMESTAMP())
```

**Include service accounts:**
```sql
-- Remove these lines from object_access CTE:
AND ah.USER_NAME NOT LIKE '%_SERVICE'
AND ah.USER_NAME NOT LIKE 'SYS_%'
```

## Wave Assignment Logic

Models are assigned to migration waves based on usage and complexity:

| Wave | Criteria | Recommendation |
|------|----------|----------------|
| **Wave 1** | >100 queries/90d AND ≤3 downstream dependencies | Migrate first - high usage, low risk |
| **Wave 2** | >100 queries/90d AND >3 deps, OR >20 queries/90d | Migrate second - requires coordination |
| **Wave 3** | ≤20 queries/90d | Migrate last - low priority |
| **DEPRECATE** | >90 days since last use | Consider retirement |

## Criticality Score

Each model receives a criticality score (higher = more critical):

```
Score = (queries_90d × 0.4) + (users × 15) + (dependencies × 20) + (error_penalty)
```

- **Query weight (0.4)**: Frequent usage indicates business importance
- **User weight (15)**: Multi-user models have broader impact
- **Dependency weight (20)**: Downstream objects multiply migration risk
- **Error penalty (+10)**: Models with >5% error rate need attention

## Usage Examples

### View Migration Plan

```sql
-- Top priority models
SELECT 
    model_name,
    migration_wave,
    impact_level,
    risk_level,
    criticality_score,
    migration_rationale
FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN
ORDER BY criticality_score DESC
LIMIT 20;
```

### Wave Summary

```sql
-- Models per wave with metrics
SELECT 
    migration_wave,
    COUNT(*) AS model_count,
    SUM(total_queries_90d) AS total_queries,
    SUM(distinct_users_90d) AS total_users,
    AVG(criticality_score)::INT AS avg_criticality
FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN
GROUP BY migration_wave
ORDER BY migration_wave;
```

### High-Risk Models

```sql
-- Models with high usage and many dependencies
SELECT *
FROM MIGRATION_PLANNING.ANALYTICS.MODEL_USAGE_METRICS
WHERE total_queries_90d > 100
  AND downstream_object_count > 3
ORDER BY criticality_score DESC;
```

### Stale Models (Cleanup Candidates)

```sql
-- Unused models with dependencies (potential cleanup)
SELECT *
FROM MIGRATION_PLANNING.ANALYTICS.MODEL_USAGE_METRICS
WHERE days_since_last_use > 60
  AND downstream_object_count > 0
ORDER BY downstream_object_count DESC;
```

### Performance Hotspots

```sql
-- Slow, frequently-used models
SELECT 
    model_name,
    total_queries_90d,
    avg_execution_time_ms,
    p95_execution_time_ms
FROM MIGRATION_PLANNING.ANALYTICS.MODEL_USAGE_METRICS
WHERE avg_execution_time_ms > 1000
  AND total_queries_90d > 50
ORDER BY avg_execution_time_ms DESC;
```

## Using with Cortex Analyst

The semantic view enables natural language queries. Use it with Snowflake Intelligence or the SEMANTIC_VIEW function:

**Example natural language queries:**
- "How many models are in Wave 1?"
- "Show me high impact models"
- "Which models have the most dependencies?"
- "What's the average criticality score by wave?"

## Customization

### Change Target Database/Schema

Edit `sql/deploy.sql` and replace all occurrences of:
- `MIGRATION_PLANNING` → your database name
- `ANALYTICS` → your schema name

### Adjust Wave Thresholds

Modify the CASE statement in `sql/deploy.sql` (Step 3a):

```sql
CASE
    WHEN m.days_since_last_use > 90 THEN 'DEPRECATE'
    WHEN m.total_queries_90d > 100 AND m.downstream_object_count <= 3 THEN 'Wave 1'
    -- Adjust thresholds as needed
    ...
END AS migration_wave
```

### Modify Criticality Score Weights

Update the score calculation in Step 2 of `sql/deploy.sql`:

```sql
ROUND(
    (q.total_queries_90d * 0.4) +      -- Query frequency weight
    (q.distinct_users_90d * 15) +       -- User impact weight
    (COALESCE(d.downstream_object_count, 0) * 20) +  -- Dependency weight
    (CASE WHEN q.error_rate > 0.05 THEN 10 ELSE 0 END),  -- Error penalty
2) AS criticality_score
```

### Filter Specific Databases/Schemas

Add WHERE clauses to the `object_access` CTE in Step 2:

```sql
WHERE ah.QUERY_START_TIME >= DATEADD('day', -90, CURRENT_TIMESTAMP())
  AND obj.value:objectDomain::STRING IN ('Table', 'View', 'Materialized View')
  -- Add your filters:
  AND SPLIT_PART(obj.value:objectName::STRING, '.', 1) IN ('MY_DB_1', 'MY_DB_2')
  AND SPLIT_PART(obj.value:objectName::STRING, '.', 2) NOT IN ('STAGING', 'TEMP')
```

## Refreshing the Migration Plan

The `MIGRATION_PLAN` table is a point-in-time snapshot. To refresh:

```sql
-- Re-run the table creation (Step 3b in deploy.sql)
CREATE OR REPLACE TABLE MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN AS
SELECT ... -- (full query from deploy.sql)
```

Or set up a scheduled task:

```sql
CREATE OR REPLACE TASK MIGRATION_PLANNING.ANALYTICS.REFRESH_MIGRATION_PLAN
  WAREHOUSE = <your_warehouse>
  SCHEDULE = 'USING CRON 0 6 * * 1 America/Los_Angeles'  -- Weekly Monday 6am
AS
  CREATE OR REPLACE TABLE MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN AS ...;
```

## Important Notes

### Data Latency

- **ACCESS_HISTORY**: Up to 3 hours latency
- **OBJECT_DEPENDENCIES**: Up to 3 hours latency
- **QUERY_HISTORY**: Up to 45 minutes latency

### Cost Considerations

- **AI_CLASSIFY**: ~0.001 credits per call
- **AI_COMPLETE**: ~0.01-0.05 credits per call (varies by response length)
- Initial deployment with 100+ models may use 5-20 credits for AI rationale generation

### Enterprise Edition Requirement

ACCESS_HISTORY is only available in Enterprise Edition and higher. Without it, you cannot track object-level access patterns. Alternative approaches for Standard Edition:
- Use QUERY_HISTORY text parsing (less accurate)
- Use OBJECT_DEPENDENCIES only (no usage metrics)

## File Structure

```
migration-prioritization-project/
├── README.md                           # This file
└── sql/
    ├── deploy.sql                      # Master deployment script
    ├── 01_model_usage_metrics.sql      # Step 1: Usage metrics table
    ├── 02_cortex_migration_recommendations.sql  # Step 2-3: AI scoring
    ├── 03_semantic_view.sql            # Step 4: Semantic view
    └── 03_sample_queries.sql           # Example analysis queries
```

## Troubleshooting

### "ACCESS_HISTORY does not exist"
Your account is not Enterprise Edition. Contact Snowflake to upgrade.

### "Insufficient privileges on SNOWFLAKE.ACCOUNT_USAGE"
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
```

### "AI_CLASSIFY/AI_COMPLETE not found"
```sql
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;
```

### No models appearing in results
- Check that the 90-day lookback window has data
- Verify your role has access to the databases being analyzed
- System users (SYSTEM, SNOWFLAKE, *_SERVICE) are excluded by default

## Support

For questions or issues:
- Review [Snowflake ACCOUNT_USAGE documentation](https://docs.snowflake.com/en/sql-reference/account-usage)
- Review [Snowflake AISQL documentation](https://docs.snowflake.com/en/sql-reference/functions/ai_classify)
- Review [Semantic Views documentation](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
