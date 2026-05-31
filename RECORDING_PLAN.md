# Spark Optimization Course — Video Recording Plan (Spark 4 Update)

## Legend

- **KEEP** = no recording work, video is unchanged
- **PATCH** = record a short insert (30s–2min) to splice into the existing video
- **RE-RECORD** = record the full video from scratch
- **NEW** = brand new video to record

---

## Part 1 — Recap

### ScalaRecap.scala — KEEP

Pure Scala language features. Nothing version-specific.

### SparkRecap.scala — PATCH

**What to add:** A 1–2 minute segment at the end covering ANSI mode as the new default in Spark 4. Show that `CAST('abc' AS INT)` and `2147483647 + 1` now throw exceptions instead of returning null. Introduce the `try_*` family (`try_cast`, `try_add`, `try_divide`) as the replacement pattern. This sets up students for the rest of the course.

**Where to splice:** After the existing content, before the `main` method demo.

---

## Part 2 — Foundations

### ReadingDAGs.scala — KEEP

DAG concepts are unchanged. The visual DAG structure in the Spark UI is the same.

### ReadingQueryPlans.scala — PATCH

**What to add:** A 1-minute note explaining that all plans in Spark 4 are wrapped in `AdaptiveSparkPlan isFinalPlan=false` because AQE is on by default. Explain that students will see this wrapper on every plan going forward, and that it means Spark may re-optimize the plan at runtime. Cross-reference the new AQE video.

**Where to splice:** At the beginning, right after the first `explain()` call, when the plan appears on screen.

### SparkAPIs.scala — PATCH

**What to add:** A 30-second verbal mention at the end: "In Spark 4, there's a new middle ground between native expressions and opaque lambdas — SQL UDFs. We'll cover those in a dedicated lesson later in the course." No code changes needed.

**Where to splice:** After the Lesson 3 conclusion ("Lambdas are impossible to optimize").

### SparkJobAnatomy.scala — KEEP

Job/stage/task concepts are unchanged. Shuffle boundaries work the same way.

### TestDeployApp.scala — PATCH

**What to add:** A 30-second note that Spark 4 requires JDK 17+ (course uses JDK 21), and that `javax` servlet APIs have moved to `jakarta` (relevant if students have custom Hadoop/Hive configs).

**Where to splice:** During the spark-submit configuration discussion (method 3 comment block).

---

## Part 3 — DataFrame Joins

### JoinsRecap.scala — KEEP

Join types are unchanged. No version-specific content.

### BroadcastJoins.scala — PATCH

**What to add:** A 1–2 minute segment explaining that AQE can now auto-convert SortMergeJoin to BroadcastHashJoin at runtime, even when `autoBroadcastJoinThreshold` is off. The manual `broadcast()` hint is still useful as a guarantee, but AQE makes it less critical. Cross-reference the AQE lesson.

**Where to splice:** After the `joinedSmart.explain()` demo, before auto-broadcast detection. Show the same join with AQE's runtime conversion in the Spark UI.

### Bucketing.scala — PATCH

**What to add:** A 1-minute note at the end about Storage Partition Join (SPJ) for V2 DataSources (Iceberg, Delta). Mention `spark.sql.sources.v2.bucketing.enabled` and `clusterBy()` as the V2-native equivalent of `bucketBy()`. No code demo needed — just frame it as the modern evolution of bucketing.

**Where to splice:** After the bucket pruning section, before `main`.

### ColumnPruning.scala — PATCH

**What to add:** A 30-second verbal note that column pruning has a runtime cousin called Dynamic Partition Pruning (DPP), covered in a later lesson. DPP prunes partitions (not columns) based on values from the other side of a join.

**Where to splice:** At the end, after the lesson summary comment.

### PrePartitioning.scala — KEEP

Pre-partitioning technique is timeless. No version-specific content.

### SkewedJoins.scala — PATCH

**What to add:** A 1–2 minute segment explaining that AQE now handles skew joins automatically (`spark.sql.adaptive.skewJoin.enabled`, default true). AQE detects skewed partitions at shuffle boundaries and splits them. The manual explode technique is still valuable when: (1) AQE can't detect the skew (pre-aggregated/pre-partitioned data), (2) you need deterministic behavior. Cross-reference the AQE lesson.

**Where to splice:** After the `joined2.explain()` comparison, before `main`. Frame it as: "This is the manual technique — let's acknowledge Spark can now do some of this automatically."

---

## Part 4 — RDD Joins

### SimpleRDDJoins.scala — KEEP

RDD join optimization techniques are unchanged.

### RDDBroadcastJoins.scala — KEEP

`sc.broadcast()` + `mapPartitions` pattern is unchanged.

### RDDSkewedJoins.scala — KEEP

RDD skew handling via explode is unchanged.

### CogroupingRDDs.scala — KEEP

`cogroup()` API is unchanged.

**Optional:** Consider recording a 30-second intro note for the Part 4 section: "RDD-level optimization is increasingly niche. Most new Spark features (AQE, DPP, SPJ, SQL UDFs) only benefit the DataFrame/SQL API. We teach both because legacy RDD code still exists in production, but new projects should default to DataFrames."

---

## Part 5 — RDD Transformations

### ByKeyFunctions.scala — KEEP

`reduceByKey` vs `groupByKey` etc. — unchanged, version-independent.

### I2ITransformations.scala — KEEP

Iterator-to-iterator transformations — unchanged, version-independent.

### ReusingObjects.scala — KEEP

Object reuse patterns — JVM-level, version-independent.

---

## Part 6 — Catalyst Optimizations (NEW SECTION)

### AdaptiveQueryExecution.scala — NEW (record from scratch)

**Estimated length:** 15–20 minutes.

**Content to cover:**
1. What AQE is and why it matters (on by default since 3.2)
2. Demo: coalescing shuffle partitions — show how AQE merges small partitions, eliminating manual `spark.sql.shuffle.partitions` tuning
3. Demo: dynamic join strategy switching — disable auto-broadcast, show SortMergeJoin in initial plan, then show AQE converts to BroadcastHashJoin at runtime in the Spark UI
4. Demo: skew join optimization — create skewed data, show AQE splitting skewed partitions. Contrast with the manual explode technique from Part 3
5. Demo: empty relation detection — filter to zero rows, show AQE short-circuits the join
6. When AQE is NOT enough — pre-partitioned skewed data where the skew is baked in before the shuffle boundary
7. Reading AQE plans — `AdaptiveSparkPlan isFinalPlan=false/true`, `CustomShuffleReader`

### DynamicPartitionPruning.scala — NEW (record from scratch)

**Estimated length:** 12–15 minutes.

**Content to cover:**
1. Static vs dynamic partition pruning — literal filter vs runtime subquery filter
2. Setup: write a partitioned Parquet fact table + small dimension table
3. Demo: static pruning — filter on partition column directly, show `PartitionFilters` in plan
4. Demo: DPP in action — join fact with dimension, filter on dimension side, show `DynamicPruningExpression` in plan and that only matching partitions are scanned
5. Demo: without DPP — disable it, show full scan for contrast
6. Demo: multi-key DPP (Spark 4 improvement) — compound partition keys
7. When DPP does NOT apply — non-partitioned tables, non-equi joins, filters on fact side

### PredicatePushdown.scala — NEW (record from scratch)

**Estimated length:** 12–15 minutes.

**Content to cover:**
1. What predicate pushdown is — filters pushed into the data source before rows enter Spark
2. Demo: Parquet pushdown — show `PushedFilters` in the plan with column statistics
3. Demo: CSV no-pushdown — same filter on CSV, show empty `PushedFilters` for contrast
4. Demo: pushdown through joins — filter on one side pushes into both sides of the join
5. Demo: UDFs block pushdown — Scala UDF makes the filter opaque, compare with native expression. Mention SQL UDFs (next lesson) as the solution
6. Demo: filter ordering — Catalyst reorders predicates, but UDF-based filters need manual ordering
7. Demo: combined pushdown + column pruning — show both `PushedFilters` and `ReadSchema` minimized together

### SqlUDFs.scala — NEW (record from scratch)

**Estimated length:** 12–15 minutes.

**Content to cover:**
1. Recap the problem: lambdas and Scala UDFs are opaque black boxes to Catalyst (callback to SparkAPIs lesson)
2. Demo: Scala UDF — register a profit UDF, show it's a black box in the plan, no pushdown
3. Demo: SQL UDF (Spark 4) — `CREATE FUNCTION profit(...) RETURNS ... RETURN ...`, show Catalyst inlines it, plan is identical to native expression
4. Demo: native expression baseline — compare all three query plans side by side
5. Demo: complex SQL UDF — CASE WHEN logic, still fully inlined
6. Demo: SQL UDF composition — one SQL UDF calling another, entire chain inlined
7. Demo: SQL UDFs from DataFrame API — using `expr()` to call SQL UDFs in Scala code
8. When Scala UDFs are still needed — external libraries, imperative logic

---

## Playground

### Playground.scala — KEEP

Setup verification only. Works with any Spark version.

---

## Summary

| Action     | Count | Videos                                                                                   |
|------------|-------|------------------------------------------------------------------------------------------|
| KEEP       | 14    | ScalaRecap, ReadingDAGs, SparkJobAnatomy, JoinsRecap, PrePartitioning, SimpleRDDJoins, RDDBroadcastJoins, RDDSkewedJoins, CogroupingRDDs, ByKeyFunctions, I2ITransformations, ReusingObjects, Playground, TestDeployApp (minor patch possible but not required) |
| PATCH      | 6     | SparkRecap, ReadingQueryPlans, SparkAPIs, BroadcastJoins, ColumnPruning, SkewedJoins     |
| RE-RECORD  | 0     |                                                                                          |
| NEW        | 4     | AdaptiveQueryExecution, DynamicPartitionPruning, PredicatePushdown, SqlUDFs               |
| REMOVE     | 0     |                                                                                          |

**Patches:** ~8 minutes total insert footage across 6 videos.
**New recordings:** ~50–65 minutes total across 4 new videos.

### Suggested recording order

1. **PredicatePushdown** — foundational, no dependencies
2. **DynamicPartitionPruning** — builds on pushdown concepts
3. **AdaptiveQueryExecution** — references skewed joins (Part 3) and broadcast joins (Part 3)
4. **SqlUDFs** — references SparkAPIs (Part 2) and predicate pushdown
5. **Patches** — record all patches last, since they cross-reference the new videos

### Bucketing note

The Bucketing lesson writes to `spark-warehouse/` using `saveAsTable`. After upgrading to Spark 4, the Hive metastore format may differ. Re-run the bucketing setup before recording the patch to verify the table creation still works as expected.
