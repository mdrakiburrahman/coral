# Coral Incremental View Maintenance: Deep Dive Research

## Executive Summary

Coral's incremental view maintenance (IVM) is an **explicit, opt-in, library-based SQL rewriter** — it does **NOT** use a Spark Plugin, SparkSessionExtensions, or any query interception mechanism. The caller explicitly invokes Coral's `RelNodeIncrementalTransformer` (or the REST API) to take a SQL query, rewrite it to an incremental version, and then the caller executes the rewritten query themselves. The system currently supports a **limited subset** of SQL operators (SELECT, JOIN, FILTER, PROJECT, UNION, AGGREGATE) and has **no built-in fallback to full refresh** — unsupported operators will either throw exceptions or produce incorrect results. The dbt integration works by calling Coral Service's REST endpoint at build time, generating Spark Scala code that leverages Apache Iceberg's time-travel to compute deltas.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Does It Use a Spark Plugin?](#2-does-it-use-a-spark-plugin)
3. [Self-Contained Spark Job Example](#3-self-contained-spark-job-example)
4. [AST Operator Coverage & Correctness](#4-ast-operator-coverage--correctness)
5. [Fallback to Full Refresh](#5-fallback-to-full-refresh)
6. [dbt Integration & Dialect Translation](#6-dbt-integration--dialect-translation)
7. [Transparent Interception vs Explicit Invocation](#7-transparent-interception-vs-explicit-invocation)
8. [Production Usages](#8-production-usages)
9. [Confidence Assessment](#9-confidence-assessment)
10. [Footnotes](#10-footnotes)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CORAL INCREMENTAL                            │
│                                                                      │
│  ┌──────────────┐    ┌─────────────────────────┐    ┌────────────┐  │
│  │ SQL String   │───▶│ HiveToRelConverter      │───▶│ RelNode    │  │
│  │ (Hive/Spark) │    │ or TrinoToRelConverter   │    │ (Calcite)  │  │
│  └──────────────┘    └─────────────────────────┘    └─────┬──────┘  │
│                                                           │         │
│                                                           ▼         │
│                                              ┌────────────────────┐ │
│                                              │ RelNodeIncremental │ │
│                                              │ Transformer        │ │
│                                              │ (AST Rewrite)      │ │
│                                              └─────────┬──────────┘ │
│                                                        │            │
│                                                        ▼            │
│                                              ┌────────────────────┐ │
│                                              │ Incremental RelNode│ │
│                                              │ (tables → _delta)  │ │
│                                              └─────────┬──────────┘ │
│                                                        │            │
│                    ┌───────────────────────────────────┬┘            │
│                    ▼                                   ▼             │
│  ┌──────────────────────────┐      ┌──────────────────────────┐    │
│  │ CoralSpark.getSparkSql() │      │ RelToTrinoConverter      │    │
│  │ → Spark SQL string       │      │ → Trino SQL string       │    │
│  └──────────────────────────┘      └──────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CORAL SERVICE (REST API)                           │
│                                                                      │
│  POST /api/translations/translate  (with rewriteType: "incremental")│
│  POST /api/incremental/rewrite     (dedicated incremental endpoint)  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CORAL-DBT (Jinja Macros)                           │
│                                                                      │
│  materialization: incremental_maintenance                            │
│  → Calls Coral Service REST API                                      │
│  → Gets incremental SQL + table name mappings                        │
│  → Generates Spark Scala using Iceberg time-travel                   │
│  → Executes via $$spark$$ block in dbt-spark                         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

The core transformation lives in a single Java class: `RelNodeIncrementalTransformer`[^1]. It operates on Apache Calcite `RelNode` trees (the logical plan representation), rewriting table scans to reference `_delta` suffixed tables and applying standard IVM delta rules for joins.

---

## 2. Does It Use a Spark Plugin?

**No.** There is absolutely no Spark Plugin, `SparkSessionExtensions`, `QueryExecutionListener`, `SparkListener`, or any form of Spark engine-level interception in this repository[^2].

The coral-spark module is purely a **SQL translation library** — it converts Calcite RelNode trees to Spark SQL strings[^3]. It does not inject itself into the Spark execution pipeline.

Key evidence:
- `grep` for `SparkPlugin`, `SparkSessionExtensions`, `spark.sql.extensions`, `QueryExecutionListener`, or `SparkListener` across the entire repository yields **zero results**[^2].
- The `coral-spark/build.gradle` declares Spark SQL as `compileOnly` (not a runtime plugin)[^4].
- The `CoralSpark` class is a standalone SQL generator, not a Spark catalyst rule or extension[^3].

---

## 3. Self-Contained Spark Job Example

Based on the dbt-generated code (from test seeds), here is what a complete self-contained Spark job looks like when using Coral Incremental with Iceberg tables:

### Step 1: Invoke Coral Service to Rewrite the Query

```bash
# Call Coral Service REST API to get the incremental rewrite
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{
    "query": "SELECT * FROM default.bar1 JOIN default.bar2 ON default.bar1.x = default.bar2.x",
    "tableNames": ["default.bar1", "default.bar2"],
    "language": "spark"
  }' \
  http://localhost:8080/api/incremental/rewrite
```

**Response:**
```json
{
  "incremental_maintenance_sql": "SELECT * FROM (SELECT * FROM default_bar1 AS bar1 INNER JOIN default_bar2_delta AS bar2_delta ON bar1.x = bar2_delta.x UNION ALL SELECT * FROM default_bar1_delta AS bar1_delta INNER JOIN default_bar2 AS bar2 ON bar1_delta.x = bar2.x) AS t UNION ALL SELECT * FROM default_bar1_delta AS bar1_delta0 INNER JOIN default_bar2_delta AS bar2_delta0 ON bar1_delta0.x = bar2_delta0.x",
  "underscore_delimited_table_names": ["default_bar1", "default_bar2"],
  "incremental_table_names": ["default_bar1_delta", "default_bar2_delta"]
}
```

### Step 2: Execute in Spark (Scala)

```scala
// === Spark Job using Coral Incremental + Iceberg ===

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("CoralIncrementalExample")
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
  .config("spark.sql.catalog.spark_catalog.type", "hive")
  .getOrCreate()

// --- For each source table, compute delta using Iceberg time-travel ---

// Table: bar1
val snapshot_df = spark.read.format("iceberg").load("bar1.snapshots")
snapshot_df.createOrReplaceTempView("snapshot_temp_table")
val snap_ids = spark.sql("SELECT snapshot_id FROM snapshot_temp_table ORDER BY committed_at DESC LIMIT 2")
val start_snapshot_id = snap_ids.collect()(1)(0).toString
val end_snapshot_id = snap_ids.collect()(0)(0).toString

// Original table state (before delta)
val df = spark.read.format("iceberg").option("snapshot-id", start_snapshot_id).load("bar1")
df.createOrReplaceTempView("default_bar1")

// Delta (new rows between snapshots)
val df_delta = spark.read.format("iceberg")
  .option("start-snapshot-id", start_snapshot_id)
  .option("end-snapshot-id", end_snapshot_id)
  .load("bar1")
df_delta.createOrReplaceTempView("default_bar1_delta")

// Table: bar2
val snapshot_df2 = spark.read.format("iceberg").load("bar2.snapshots")
snapshot_df2.createOrReplaceTempView("snapshot_temp_table")
val snap_ids2 = spark.sql("SELECT snapshot_id FROM snapshot_temp_table ORDER BY committed_at DESC LIMIT 2")
val start_snapshot_id2 = snap_ids2.collect()(1)(0).toString
val end_snapshot_id2 = snap_ids2.collect()(0)(0).toString

val df2 = spark.read.format("iceberg").option("snapshot-id", start_snapshot_id2).load("bar2")
df2.createOrReplaceTempView("default_bar2")

val df2_delta = spark.read.format("iceberg")
  .option("start-snapshot-id", start_snapshot_id2)
  .option("end-snapshot-id", end_snapshot_id2)
  .load("bar2")
df2_delta.createOrReplaceTempView("default_bar2_delta")

// --- Execute the incremental query (from Coral's rewrite) ---
val query_response = spark.sql("""
  SELECT *
  FROM (SELECT *
        FROM default_bar1 AS bar1
        INNER JOIN default_bar2_delta AS bar2_delta ON bar1.x = bar2_delta.x
        UNION ALL
        SELECT *
        FROM default_bar1_delta AS bar1_delta
        INNER JOIN default_bar2 AS bar2 ON bar1_delta.x = bar2.x) AS t
  UNION ALL
  SELECT *
  FROM default_bar1_delta AS bar1_delta0
  INNER JOIN default_bar2_delta AS bar2_delta0 ON bar1_delta0.x = bar2_delta0.x
""")

// --- Append incremental results to output table ---
query_response.write.mode("append").saveAsTable("join_output")
```

This example is derived directly from the test seed files[^5][^6].

### Key SparkConf Requirements

| Config | Value | Purpose |
|--------|-------|---------|
| `spark.sql.extensions` | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` | Iceberg support |
| `spark.sql.catalog.spark_catalog` | `org.apache.iceberg.spark.SparkSessionCatalog` | Iceberg catalog |
| `spark.sql.catalog.spark_catalog.type` | `hive` | Use Hive metastore |

Note: The Iceberg extensions are for **Iceberg** time-travel (delta computation), NOT for Coral itself. Coral has no Spark config/extension requirements.

---

## 4. AST Operator Coverage & Correctness

### Supported Operators

The `RelNodeIncrementalTransformer` implements `RelShuttleImpl` visitor methods for these operators[^1]:

| Operator | Supported | Delta Rule Applied |
|----------|-----------|-------------------|
| `TableScan` | ✅ | Rename table → `table_delta` |
| `LogicalFilter` | ✅ | Pass filter through to transformed child |
| `LogicalProject` | ✅ | Pass projection through to transformed child |
| `LogicalJoin` | ✅ | 3-term: `(R⋈ΔS) ∪ (ΔR⋈S) ∪ (ΔR⋈ΔS)` |
| `LogicalUnion` | ✅ | Transform each child independently |
| `LogicalAggregate` | ✅ | Re-aggregate over transformed child (group-level recompute) |
| `LogicalSort` | ❌ | Not implemented |
| `LogicalWindow` | ❌ | Not implemented |
| `LogicalMinus` (EXCEPT) | ❌ | Not implemented |
| `LogicalIntersect` | ❌ | Not implemented |
| `Subqueries/CTEs` | ⚠️ | Resolved at parse time by Calcite (inlined), so works transitively |
| `LEFT/RIGHT/FULL OUTER JOIN` | ❌ | Only INNER JOIN tested |
| `DISTINCT` | ⚠️ | Via `LogicalAggregate` (set semantics via UNION without ALL) |

### Correctness Approach

1. **Unit Tests**: The `RelToIncrementalSqlConverterTest` validates specific SQL patterns[^7]:
   - Simple SELECT ALL
   - Simple JOIN
   - JOIN with WHERE filter
   - JOIN with nested filter (CTE pattern)
   - Nested JOINs (3-table join)
   - UNION
   - SELECT specific columns
   - SELECT specific columns with JOIN

2. **Delta Rule Correctness**: The join delta rule implements the standard algebraic identity[^1]:
   ```
   Δ(R ⋈ S) = (R ⋈ ΔS) ∪ (ΔR ⋈ S) ∪ (ΔR ⋈ ΔS)
   ```
   This is mathematically correct for **append-only** changes (insertions only). It does NOT handle deletions or updates.

3. **No Performance Testing**: There are no benchmarks, performance tests, or cost-based decisions in the incremental module.

### Key Limitation: Insert-Only Assumption

The entire system assumes **append-only deltas**. The `_delta` tables contain only new rows. There is no concept of:
- Negative deltas (deletions)
- Update tracking
- Retraction handling

This is evidenced by the Iceberg integration which uses `start-snapshot-id`/`end-snapshot-id` to get only added rows[^5][^6].

---

## 5. Fallback to Full Refresh

### **There is NO built-in fallback to full refresh.**

The `RelNodeIncrementalTransformer` does not implement any fallback mechanism[^1]. If it encounters an unsupported RelNode type, the default `RelShuttleImpl` behavior will:
- Either pass through the node unchanged (potentially producing incorrect results)
- Or throw a runtime exception

Evidence:
- The `RelShuttleImpl` base class has default implementations that return the node as-is for unhandled cases
- There is no try/catch wrapper or "canIncrementalize" check anywhere
- The dbt materialization `incremental_maintenance.sql` does not have fallback logic[^8]
- The Coral Service `/api/incremental/rewrite` endpoint returns an error on failure, not a fallback query[^9]

### What Happens with Unsupported SQL?

```
SQL with WINDOW function
  → HiveToRelConverter → RelNode with LogicalWindow
  → RelNodeIncrementalTransformer
  → LogicalWindow has no visitor override
  → Default RelShuttleImpl.visit(RelNode) returns the node unchanged
  → Child nodes ARE transformed (table scans become _delta)
  → Result: INCORRECT incremental query (window over deltas only, not full state)
```

**This means the system can silently produce wrong results for unsupported patterns.** There is no guarantee it will rewrite ANY SQL query 100% incrementally — it only correctly handles the subset of operators with explicit visitor implementations.

---

## 6. dbt Integration & Dialect Translation

### How Does dbt Know What Dialect to Translate?

The dialect is **explicitly specified by the caller** in the dbt macro invocation. The `incremental_maintenance` materialization hardcodes the dialect to `'spark'`[^8]:

```sql
{% set sql_dialect = 'spark' %}
{% set coral_response = coral_dbt.get_coral_incremental_response(
    sql, coral_incremental_url, table_names, sql_dialect) %}
```

The request sent to Coral Service includes a `language` field[^10]:
```json
{
  "query": "SELECT * FROM db.t1 UNION SELECT * FROM db.t2",
  "tableNames": ["db.t1", "db.t2"],
  "language": "spark"
}
```

The `IncrementalUtils.getIncrementalQuery()` method uses this to select the appropriate parser and generator[^11]:
- `sourceLanguage` determines which parser to use (Hive or Trino)
- `targetLanguage` determines which generator to use (CoralSpark or RelToTrinoConverter)

For the `trino_table` materialization, the dialect is similarly explicit[^12]:
```sql
{% set request_data = {
    "fromLanguage": "trino",
    "toLanguage": "spark",
    "query": sql
} %}
```

### dbt Integration Flow

```
dbt model file (SQL + config)
        │
        ▼
┌───────────────────────────────────┐
│ incremental_maintenance           │
│ materialization macro             │
│                                   │
│ 1. Read model SQL                 │
│ 2. Read table_names config        │
│ 3. POST to Coral Service          │
│    /api/incremental/rewrite       │
│ 4. Receive incremental SQL +      │
│    table name mappings            │
│ 5. Generate Spark Scala code      │
│    (Iceberg time-travel + query)  │
│ 6. Execute via $$spark$$ block    │
└───────────────────────────────────┘
```

### dbt Model Example

```sql
-- models/my_incremental_view.sql
{{
  config(
    materialized='incremental_maintenance',
    table_names=['db.t1', 'db.t2'],
  )
}}

SELECT * FROM db.t1 UNION SELECT * FROM db.t2
```

The `table_names` config is **required** — dbt does not auto-discover dependencies for this materialization[^13].

---

## 7. Transparent Interception vs Explicit Invocation

### **It is 100% explicit/opt-in from the caller.**

There is no transparent query interception at any level:

| Mechanism | Present? | Evidence |
|-----------|----------|----------|
| Spark Plugin | ❌ | No `SparkPlugin` class anywhere[^2] |
| SparkSessionExtensions | ❌ | No extension registration[^2] |
| Catalyst Rule injection | ❌ | No `Rule[LogicalPlan]` implementations[^2] |
| QueryExecutionListener | ❌ | No listener implementations[^2] |
| SparkListener | ❌ | No listener implementations[^2] |
| Spark SQL config extension | ❌ | No `spark.sql.extensions` reference[^2] |
| JDBC driver wrapper | ❌ | Not present |

### The Caller's Responsibility

The caller must:
1. **TAKE** the original SQL query
2. **SEND** it to Coral (via library call or REST API)
3. **RECEIVE** the rewritten incremental SQL
4. **SET UP** delta temp views (using Iceberg time-travel or other mechanism)
5. **EXECUTE** the rewritten SQL themselves
6. **MERGE** the results into the target table

In code form:
```java
// EXPLICIT: caller invokes Coral
RelNode originalNode = new HiveToRelConverter(hmsClient).convertSql(originalSql);
RelNode incrementalNode = RelNodeIncrementalTransformer.convertRelIncremental(originalNode);
CoralSpark coralSpark = CoralSpark.create(incrementalNode, hmsClient);
String incrementalSparkSql = coralSpark.getSparkSql();
// Now caller must execute incrementalSparkSql themselves
```

Or via REST:
```bash
curl -X POST http://localhost:8080/api/incremental/rewrite \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT ...", "tableNames":["t1","t2"], "language":"spark"}'
```

### Can You Hook It into Any Spark Job Transparently?

**No.** You cannot simply add a JAR and have all queries automatically incrementalized. The closest you could get would be:
1. Writing your own `SparkSessionExtensions` that wraps Coral
2. Intercepting logical plans in Catalyst
3. Calling Coral's transformer on captured plans
4. Replacing the original plan with the incremental one

But **none of this exists in the Coral repository**. It would require significant custom engineering.

---

## 8. Production Usages

### 8.1 This Repository's Unit/Integration Tests

| Test File | What It Tests |
|-----------|---------------|
| `coral-incremental/src/test/java/.../RelToIncrementalSqlConverterTest.java`[^7] | Core incremental transformer: 8 test cases covering SELECT, JOIN, nested JOIN, UNION, filters |
| `coral-incremental/src/test/java/.../TestUtils.java`[^14] | Test setup: creates Hive tables, configures HiveToRelConverter |
| `coral-dbt/src/main/resources/tests/test_incremental.py`[^15] | dbt macro tests: validates generated Spark Scala against expected outputs |
| `coral-dbt/src/main/resources/tests/seeds/test_simple_select_all_expected.txt`[^5] | Expected Iceberg-based Spark Scala for simple SELECT |
| `coral-dbt/src/main/resources/tests/seeds/test_join_expected.txt`[^6] | Expected Iceberg-based Spark Scala for JOIN |

### 8.2 Other Repositories That Use Coral Incremental

Based on GitHub-wide search, **no external repositories directly use `coral-incremental`**[^16]. The search for `RelNodeIncrementalTransformer` outside linkedin/coral returns zero results.

Repositories that reference Coral (but for translation, not incremental):
- [GuinsooLab/witdb](https://github.com/GuinsooLab/witdb) — includes `com.linkedin.coral` as a dependency (Trino fork)
- Various Trino forks (Vaishnav-puram/trino, BOFA1ex/trino) — use coral-trino for dialect translation
- [isaccanedo/coral](https://github.com/isaccanedo/coral) — fork of linkedin/coral

### 8.3 Related IVM Projects (Not Using Coral Directly)

| Project | Relationship to Coral |
|---------|-----------------------|
| [ila/openivm](https://github.com/ila/openivm) | DuckDB-based IVM extension. References Coral in its documentation as a comparison point. Implements more complete IVM (handles deletes, updates, window functions, outer joins). Not built on Coral[^17]. |
| LinkedIn internal (production) | The Coral README and slideshare presentation reference LinkedIn's internal use for "incremental view maintenance with Coral, DBT, and Iceberg"[^18]. This is the primary production usage, but the internal implementation is not public. |

### 8.4 dbt and Spark Usage

The dbt integration exists in `coral-dbt/`[^13] and is specifically designed for:
- **Engine**: Apache Spark (via dbt-spark adapter)
- **Table format**: Apache Iceberg (for time-travel based delta computation)
- **Orchestrator**: dbt (for DAG management and materialization)

**Status**: The README explicitly states "Note: This project is currently WIP"[^13].

**Required Setup** (non-trivial):
1. Modified dbt-core source (to expose `requests` module in Jinja context)[^13]
2. Running Coral Service instance
3. Spark with Iceberg support
4. Tables must be Iceberg tables (for snapshot-based time-travel)

---

## 9. Confidence Assessment

| Claim | Confidence | Basis |
|-------|------------|-------|
| No Spark Plugin exists | **Certain** | Exhaustive grep of entire codebase[^2] |
| Explicit opt-in (not transparent) | **Certain** | Architecture review of all entry points |
| Limited operator coverage | **Certain** | Source code of `RelNodeIncrementalTransformer`[^1] |
| No fallback to full refresh | **Certain** | No fallback logic in transformer or service |
| Insert-only assumption | **Certain** | Iceberg integration only reads appended rows[^5][^6] |
| dbt integration is WIP | **Certain** | Explicitly stated in README[^13] |
| No external production users (public) | **High** | GitHub-wide search returned zero[^16] |
| LinkedIn uses it internally in production | **Moderate** | Referenced in conference talks[^18] but no public evidence of internal codebase |
| Aggregate handling is group-level recompute | **High** | Source shows aggregate just wraps transformed child, re-aggregating all[^1] |

### Assumptions Made
- The `master` branch represents the current state of the project
- The dbt integration requires the exact setup described in the README
- "WIP" status means incomplete feature coverage

---

## 10. Footnotes

[^1]: `coral-incremental/src/main/java/com/linkedin/coral/incremental/RelNodeIncrementalTransformer.java` — entire file (109 lines), the core transformer implementation

[^2]: Exhaustive `grep` for `SparkPlugin|SparkSessionExtensions|spark.sql.extensions|QueryExecutionListener|SparkListener` across entire repository returned zero matches

[^3]: `coral-spark/src/main/java/com/linkedin/coral/spark/CoralSpark.java:76-84` — `create()` method showing it's a pure SQL generator

[^4]: `coral-spark/build.gradle:8` — `compileOnly deps.'spark'.'sql'` (not a runtime dependency)

[^5]: `coral-dbt/src/main/resources/tests/seeds/test_simple_select_all_expected.txt` — expected Spark Scala output for simple SELECT

[^6]: `coral-dbt/src/main/resources/tests/seeds/test_join_expected.txt` — expected Spark Scala output for JOIN with Iceberg time-travel

[^7]: `coral-incremental/src/test/java/com/linkedin/coral/incremental/RelToIncrementalSqlConverterTest.java` — 8 test methods

[^8]: `coral-dbt/src/main/resources/macros/coral_macros/spark/materializations/incremental_maintenance.sql:24-26` — materialization implementation

[^9]: `coral-service/src/main/java/com/linkedin/coral/coralservice/controller/TranslationController.java:146-150` — error handling returns 500, no fallback

[^10]: `coral-service/src/main/java/com/linkedin/coral/coralservice/entity/IncrementalRequestBody.java` — request body with `language` field

[^11]: `coral-service/src/main/java/com/linkedin/coral/coralservice/utils/IncrementalUtils.java:20-44` — dialect-based routing

[^12]: `coral-dbt/src/main/resources/macros/coral_macros/spark/utils/trino_to_spark.sql:8-16` — explicit fromLanguage/toLanguage

[^13]: `coral-dbt/README.md` — full setup instructions and "WIP" status

[^14]: `coral-incremental/src/test/java/com/linkedin/coral/incremental/TestUtils.java` — test infrastructure

[^15]: `coral-dbt/src/main/resources/tests/test_incremental.py` — Python unittest for dbt macros

[^16]: GitHub code search for `RelNodeIncrementalTransformer NOT repo:linkedin/coral` — zero results

[^17]: [ila/openivm](https://github.com/ila/openivm) README and `.claude/skills/dbt-coral-reference/SKILL.md` — references Coral as comparison

[^18]: `README.md:91` — reference to [Incremental View Maintenance with Coral, DBT, and Iceberg](https://www.slideshare.net/walaa_eldin_moustafa/incremental-view-maintenance-with-coral-dbt-and-iceberg) (Iceberg Meetup, 5/11/2023)

---

## Summary Comparison Table

| Question | Answer |
|----------|--------|
| Uses Spark Plugin? | **No** |
| Transparent interception? | **No** — fully explicit/opt-in |
| Guaranteed to rewrite ANY SQL? | **No** — limited to SELECT, JOIN, FILTER, PROJECT, UNION, AGGREGATE |
| Has fallback to full refresh? | **No** — can silently produce wrong results for unsupported operators |
| How does dbt know dialect? | **Explicitly specified** in materialization config (`language: 'spark'`) |
| Can hook into any Spark job? | **No** — requires explicit API call from caller |
| External production users? | **None found publicly** (LinkedIn internal usage referenced in talks) |
