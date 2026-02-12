# Migration Prioritization with Snowflake Cortex

A technical solution for automatically prioritizing data models for migration using Snowflake's observability views and Cortex AI functions.

---

## Table of Contents

1. [Technical Solution Overview](#1-technical-solution-overview)
2. [Design Details](#2-design-details)
3. [Deployment Guide](#3-deployment-guide)
4. [Known Limitations](#4-known-limitations)

---

## 1. Technical Solution Overview

### Problem Statement

Organizations migrating data platforms need to prioritize which models (tables, views) to migrate first. Manual prioritization is time-consuming and error-prone. This solution automates prioritization by analyzing actual usage patterns, user impact, and downstream dependencies.

### Solution Approach

The solution follows a 4-step pattern that leverages Snowflake's built-in observability and AI capabilities:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │ ACCESS_HISTORY  │  │ QUERY_HISTORY   │  │ OBJECT_DEPENDENCIES         │  │
│  │ Who queried     │  │ How it performed│  │ What depends on it          │  │
│  │ what, when      │  │ (time, bytes)   │  │ (downstream objects)        │  │
│  └────────┬────────┘  └────────┬────────┘  └──────────────┬──────────────┘  │
└───────────┼─────────────────────┼─────────────────────────┼─────────────────┘
            │                     │                         │
            └─────────────────────┼─────────────────────────┘
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 1: MODEL_USAGE_METRICS (Table)                                        │
│  ─────────────────────────────────────                                      │
│  Aggregates 90-day usage into per-object metrics:                           │
│  • Query frequency, distinct users, last access                             │
│  • Performance (avg/p95 execution time, bytes scanned)                      │
│  • Dependencies (downstream object counts)                                  │
│  • Criticality score (composite ranking)                                    │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 2: MIGRATION_SCORING (View)                                           │
│  ────────────────────────────────                                           │
│  Applies AI classification and rules-based wave assignment:                 │
│  • AI_CLASSIFY → Impact level (HIGH/MEDIUM/LOW)                             │
│  • AI_CLASSIFY → Risk level (HIGH/MEDIUM/LOW)                               │
│  • Rules → Migration wave (Wave 1/2/3/DEPRECATE)                            │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 3: MIGRATION_PLAN (Table)                                             │
│  ──────────────────────────────                                             │
│  Materializes scores with AI-generated rationales:                          │
│  • AI_COMPLETE → Human-readable migration rationale per model               │
│  • Point-in-time snapshot for reporting and export                          │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 4: MIGRATION_PLANNING_SEM (Semantic View)                             │
│  ──────────────────────────────────────────────                             │
│  Enables natural language queries via Cortex Analyst:                       │
│  • "Show me Wave 1 models"                                                  │
│  • "Which models have the most dependencies?"                               │
│  • "What's the average criticality by database?"                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **AISQL over Cortex Agents** | Batch scoring workloads benefit from AISQL's row-by-row processing; Agents are better for interactive exploration |
| **Table over View for metrics** | Snapshot semantics; avoids repeated expensive ACCOUNT_USAGE queries |
| **Rules-based wave assignment** | Deterministic, auditable; AI adds rationale, not the classification itself |
| **90-day lookback window** | Balances recency vs. sample size; configurable |

### Objects Created

| Object | Type | Purpose |
|--------|------|---------|
| `MODEL_USAGE_METRICS` | Table | Foundation metrics from ACCOUNT_USAGE |
| `MIGRATION_SCORING` | View | Real-time AI classification layer |
| `MIGRATION_PLAN` | Table | Materialized plan with AI rationales |
| `MIGRATION_PLANNING_SEM` | Semantic View | Natural language query interface |

---

## 2. Design Details

### 2.1 MODEL_USAGE_METRICS: The Foundation Layer

This table is the lynchpin of the solution. It combines three ACCOUNT_USAGE views into a single, queryable metrics layer.

#### Data Sources

| Source View | What It Provides | Key Fields |
|-------------|------------------|------------|
| `ACCESS_HISTORY` | Object-level access patterns | `BASE_OBJECTS_ACCESSED`, `USER_NAME`, `QUERY_ID` |
| `QUERY_HISTORY` | Query performance metrics | `TOTAL_ELAPSED_TIME`, `BYTES_SCANNED`, `EXECUTION_STATUS` |
| `OBJECT_DEPENDENCIES` | Downstream relationships | `REFERENCING_OBJECT_NAME`, `REFERENCING_OBJECT_DOMAIN` |

#### Query Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CTE 1: object_access                                                       │
│  ────────────────────                                                       │
│  • FLATTEN(BASE_OBJECTS_ACCESSED) to get per-object access records          │
│  • SPLIT_PART to parse DATABASE.SCHEMA.OBJECT names                         │
│  • Filter: Tables, Views, Materialized Views only                           │
│  • Filter: Exclude system users (SYSTEM, SNOWFLAKE, *_SERVICE, SYS_*)       │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CTE 2: query_metrics                                                       │
│  ────────────────────                                                       │
│  • JOIN to QUERY_HISTORY on QUERY_ID                                        │
│  • Aggregate: COUNT(queries), COUNT(users), MAX(last_used)                  │
│  • Aggregate: AVG/P95 execution time, AVG bytes scanned                     │
│  • Calculate: error_rate = failed_queries / total_queries                   │
│  • GROUP BY: database_name, schema_name, object_name, object_type           │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CTE 3: dependency_metrics                                                  │
│  ─────────────────────────                                                  │
│  • Query OBJECT_DEPENDENCIES for each referenced object                     │
│  • COUNT DISTINCT downstream objects (total, views, tables)                 │
│  • LISTAGG downstream object names for reference                            │
│  • GROUP BY: database_name, schema_name, object_name                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Final SELECT                                                               │
│  ────────────                                                               │
│  • LEFT JOIN query_metrics to dependency_metrics (case-insensitive)         │
│  • Calculate criticality_score                                              │
│  • Filter: total_queries_90d > 0 (exclude never-queried objects)            │
│  • Add: refreshed_at timestamp                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Metric Definitions

| Metric | Formula | Description |
|--------|---------|-------------|
| `total_queries_90d` | `COUNT(DISTINCT QUERY_ID)` | Unique queries accessing this object |
| `distinct_users_90d` | `COUNT(DISTINCT USER_NAME)` | Unique users querying this object |
| `last_used_at` | `MAX(QUERY_START_TIME)` | Most recent access timestamp |
| `days_since_last_use` | `DATEDIFF(day, last_used_at, NOW())` | Staleness indicator |
| `avg_execution_time_ms` | `AVG(TOTAL_ELAPSED_TIME)` | Mean query duration |
| `p95_execution_time_ms` | `PERCENTILE_CONT(0.95)` | 95th percentile duration |
| `avg_bytes_scanned` | `AVG(BYTES_SCANNED)` | Mean data read per query |
| `error_rate` | `SUM(failures) / COUNT(*)` | Query failure ratio (0.0-1.0) |
| `downstream_object_count` | `COUNT(DISTINCT dependents)` | Objects that reference this one |
| `criticality_score` | See below | Composite priority ranking |

#### Criticality Score Formula

```sql
criticality_score = 
    (total_queries_90d × 0.4) +           -- Usage frequency
    (distinct_users_90d × 15) +           -- User breadth
    (downstream_object_count × 20) +      -- Blast radius
    (CASE WHEN error_rate > 0.05 THEN 10 ELSE 0 END)  -- Stability flag
```

**Weight rationale:**
- **0.4 per query**: High-frequency objects are business-critical
- **15 per user**: Multi-user objects have broader organizational impact
- **20 per dependency**: Downstream objects multiply migration risk
- **+10 error penalty**: Unstable objects may need priority attention

### 2.2 MIGRATION_SCORING: AI Classification Layer

This view applies Snowflake's AISQL functions to classify each model's impact and risk.

#### AI_CLASSIFY for Impact Level

```sql
AI_CLASSIFY(
    'Model: ' || model_name || 
    ' | Queries: ' || total_queries_90d || 
    ' | Users: ' || distinct_users_90d || 
    ' | Dependencies: ' || downstream_object_count,
    ARRAY_CONSTRUCT('HIGH', 'MEDIUM', 'LOW')
):label::STRING AS impact_level
```

#### AI_CLASSIFY for Risk Level

```sql
AI_CLASSIFY(
    'Model: ' || model_name || 
    ' | Dependencies: ' || downstream_object_count || 
    ' | Avg exec time: ' || avg_execution_time_ms || 
    ' | Error rate: ' || error_rate ||
    ' | Days since use: ' || days_since_last_use,
    ARRAY_CONSTRUCT('HIGH', 'MEDIUM', 'LOW')
):label::STRING AS risk_level
```

#### Rules-Based Wave Assignment

```sql
CASE
    WHEN days_since_last_use > 90 THEN 'DEPRECATE'
    WHEN total_queries_90d > 100 AND downstream_object_count <= 3 THEN 'Wave 1'
    WHEN total_queries_90d > 100 AND downstream_object_count > 3 THEN 'Wave 2'
    WHEN total_queries_90d > 20 THEN 'Wave 2'
    ELSE 'Wave 3'
END AS migration_wave
```

| Wave | Criteria | Strategy |
|------|----------|----------|
| **Wave 1** | >100 queries AND ≤3 dependencies | High usage, low risk → migrate first |
| **Wave 2** | >100 queries AND >3 deps, OR >20 queries | Requires coordination |
| **Wave 3** | ≤20 queries | Low priority |
| **DEPRECATE** | >90 days since last use | Candidate for retirement |

### 2.3 MIGRATION_PLAN: Materialized Output

This table materializes the scoring view and adds AI-generated rationales.

#### AI_COMPLETE for Rationales

```sql
AI_COMPLETE(
    'claude-3-5-sonnet',
    'You are a data migration consultant. Provide a concise 1-2 sentence 
     rationale for this migration classification.
     
     Model: ' || model_name || '
     Metrics: Queries=' || total_queries_90d || ', Users=' || distinct_users_90d ||
     ', Dependencies=' || downstream_object_count || '
     Classification: Impact=' || impact_level || ', Risk=' || risk_level || 
     ', Wave=' || migration_wave
') AS migration_rationale
```

### 2.4 MIGRATION_PLANNING_SEM: Semantic View

Enables natural language queries via Cortex Analyst.

#### Dimensions (Filterable Attributes)

| Dimension | Source Column | Synonyms |
|-----------|---------------|----------|
| `database_name` | `database_name` | db, database |
| `schema_name` | `schema_name` | schema |
| `model_name` | `model_name` | model, table, view, object |
| `impact_level` | `impact_level` | impact, importance |
| `risk_level` | `risk_level` | risk, complexity |
| `migration_wave` | `migration_wave` | wave, phase, priority |

#### Metrics (Aggregatable Measures)

| Metric | Aggregation | Synonyms |
|--------|-------------|----------|
| `query_count` | `SUM(total_queries_90d)` | queries, usage |
| `model_count` | `COUNT(model_name)` | models, count |
| `dependency_count` | `SUM(downstream_object_count)` | dependencies |
| `avg_criticality` | `AVG(criticality_score)` | criticality, score |

---

## 3. Deployment Guide

### 3.1 Prerequisites

#### Snowflake Requirements

| Requirement | Details |
|-------------|---------|
| **Edition** | Enterprise Edition or higher (required for ACCESS_HISTORY) |
| **Role** | ACCOUNTADMIN or custom role with privileges below |
| **Cortex Access** | SNOWFLAKE.CORTEX_USER database role |

#### Required Privileges

```sql
-- Access to ACCOUNT_USAGE views
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;

-- Cortex AI functions
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;

-- Object creation (choose one)
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <your_role>;
-- OR for existing database:
GRANT CREATE SCHEMA ON DATABASE <your_database> TO ROLE <your_role>;
```

### 3.2 Deployment Steps

#### Option 1: Snowflake CLI (Recommended)

```bash
# Clone the repository
git clone https://github.com/sfc-gh-akelkar/migration-prioritization-project.git
cd migration-prioritization-project

# Deploy (5-15 minutes depending on model count)
snow sql -f sql/deploy.sql
```

#### Option 2: Snowsight

1. Open Snowsight → **Worksheets**
2. Create a new SQL worksheet
3. Copy contents of `sql/deploy.sql`
4. Execute all statements

### 3.3 Deployment Output

```
[STEP 1/4] Creating database and schema...
[PASS] Database MIGRATION_PLANNING.ANALYTICS created

[STEP 2/4] Creating MODEL_USAGE_METRICS table...
[PASS] MODEL_USAGE_METRICS table created with 167 models

[STEP 3/4] Creating MIGRATION_SCORING view and MIGRATION_PLAN table...
[INFO] Generating AI rationales (this may take a few minutes)...
[PASS] MIGRATION_PLAN table created

[STEP 4/4] Creating semantic view for Cortex Analyst...
[PASS] Semantic view MIGRATION_PLANNING_SEM created

[SUCCESS] Migration Prioritization deployment complete!
```

### 3.4 Customization

#### Change Target Database/Schema

Edit `sql/deploy.sql` and replace:
- `MIGRATION_PLANNING` → your database name
- `ANALYTICS` → your schema name

#### Filter to Specific Databases

Add to the `object_access` CTE:

```sql
AND SPLIT_PART(obj.value:objectName::STRING, '.', 1) IN ('PROD_DB', 'ANALYTICS_DB')
```

#### Exclude Staging Schemas

```sql
AND SPLIT_PART(obj.value:objectName::STRING, '.', 2) NOT IN ('STAGING', 'TEMP', 'RAW')
```

#### Change Lookback Window

```sql
-- From 90 days to 180 days:
WHERE ah.QUERY_START_TIME >= DATEADD('day', -180, CURRENT_TIMESTAMP())
```

#### Adjust Criticality Weights

```sql
ROUND(
    (q.total_queries_90d * 0.4) +      -- Increase for usage focus
    (q.distinct_users_90d * 15) +       -- Increase for user impact focus
    (downstream_object_count * 20) +    -- Increase for blast radius focus
    (CASE WHEN q.error_rate > 0.05 THEN 10 ELSE 0 END),
2) AS criticality_score
```

### 3.5 Refreshing Data

The `MIGRATION_PLAN` table is a point-in-time snapshot. To refresh:

```sql
-- Manual refresh
CREATE OR REPLACE TABLE MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN AS
SELECT ... -- (copy from deploy.sql Step 3b)
```

```sql
-- Scheduled refresh (weekly)
CREATE OR REPLACE TASK MIGRATION_PLANNING.ANALYTICS.REFRESH_MIGRATION_PLAN
  WAREHOUSE = <your_warehouse>
  SCHEDULE = 'USING CRON 0 6 * * 1 America/Los_Angeles'
AS
  CREATE OR REPLACE TABLE MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN AS ...;

ALTER TASK MIGRATION_PLANNING.ANALYTICS.REFRESH_MIGRATION_PLAN RESUME;
```

### 3.6 Validation Queries

After deployment, verify data quality:

```sql
-- Check row counts
SELECT COUNT(*) FROM MIGRATION_PLANNING.ANALYTICS.MODEL_USAGE_METRICS;
SELECT COUNT(*) FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN;

-- Verify wave distribution
SELECT migration_wave, COUNT(*) AS models
FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN
GROUP BY 1 ORDER BY 1;

-- Sample top-priority models
SELECT model_name, migration_wave, criticality_score, migration_rationale
FROM MIGRATION_PLANNING.ANALYTICS.MIGRATION_PLAN
ORDER BY criticality_score DESC
LIMIT 10;
```

---

## 4. Known Limitations

### 4.1 Data Source Limitations

#### Performance Metrics Are Query-Level

`avg_bytes_scanned` and `avg_rows_returned` represent **query-level** metrics, not per-object attribution. A query joining 5 tables attributes its full `BYTES_SCANNED` to all 5.

**Impact**: Directionally useful for "hot" object identification, but not precise per-object I/O.

#### Data Latency

| Source | Latency |
|--------|---------|
| ACCESS_HISTORY | Up to 3 hours |
| OBJECT_DEPENDENCIES | Up to 3 hours |
| QUERY_HISTORY | Up to 45 minutes |

#### Enterprise Edition Required

`ACCESS_HISTORY` requires Enterprise Edition. Standard Edition alternatives:
- Parse `QUERY_HISTORY.QUERY_TEXT` (less accurate)
- Use `OBJECT_DEPENDENCIES` only (no usage metrics)

### 4.2 Query Implementation Limitations

#### LISTAGG Output Size

`users_list` and `downstream_objects_list` use `LISTAGG()` with ~16MB output limit. Extremely hot objects could truncate.

**Mitigation**:
```sql
LEFT(LISTAGG(...), 8000) AS users_list
```

#### Object Name Parsing

`SPLIT_PART(objectName, '.', N)` assumes no dots in identifiers. Objects named `"my.table"` will misparse.

**Impact**: Rare in practice; document for customers with exotic naming.

#### Downstream Uniqueness

Dependencies count by object name only. Same-named objects in different schemas count correctly due to grouping, but the list doesn't show fully-qualified names.

**If needed**:
```sql
COUNT(DISTINCT REFERENCING_DATABASE || '.' || REFERENCING_SCHEMA || '.' || REFERENCING_OBJECT_NAME)
```

### 4.3 Filtering Limitations

#### Service Accounts Excluded

These patterns are excluded: `SYSTEM`, `SNOWFLAKE`, `*_SERVICE`, `SYS_*`

**Impact**: Legitimate BI/ETL accounts (e.g., `TABLEAU_SERVICE`) won't be counted. Remove filters if needed.

### 4.4 Scoring Limitations

#### Criticality Score Is Heuristic

The default weights are a **starting point**:
```
(queries × 0.4) + (users × 15) + (deps × 20) + (error_penalty)
```

Adjust based on priorities:
- **Blast radius focus**: Increase dependency weight
- **Usage focus**: Increase query/user weights
- **Stability focus**: Increase error penalty

#### AI Classification Is Non-Deterministic

`AI_CLASSIFY` may produce slightly different results on re-run. Wave assignment uses deterministic rules to ensure consistency.

### 4.5 Performance Limitations

#### Large Account Overhead

The 90-day `ACCESS_HISTORY + FLATTEN` can be resource-intensive on large accounts.

**Mitigations**:
- Restrict to specific databases/schemas
- Use Dynamic Table with scheduled refresh
- Move LISTAGG fields to separate detail table

### 4.6 Cost Considerations

| Function | Approximate Cost |
|----------|------------------|
| AI_CLASSIFY | ~0.001 credits/call |
| AI_COMPLETE | ~0.01-0.05 credits/call |
| Initial deployment (100+ models) | 5-20 credits for AI rationales |

---

## File Structure

```
migration-prioritization-project/
├── README.md                                # This design document
└── sql/
    ├── deploy.sql                           # Master deployment script
    ├── 01_model_usage_metrics.sql           # Step 1: Usage metrics table
    ├── 02_cortex_migration_recommendations.sql  # Steps 2-3: AI scoring
    ├── 03_semantic_view.sql                 # Step 4: Semantic view
    └── 03_sample_queries.sql                # Example analysis queries
```

---

## References

- [Snowflake ACCOUNT_USAGE Documentation](https://docs.snowflake.com/en/sql-reference/account-usage)
- [Snowflake AISQL Functions](https://docs.snowflake.com/en/sql-reference/functions/ai_classify)
- [Semantic Views Documentation](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
