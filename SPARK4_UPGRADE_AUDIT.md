# Spark Optimization Course — Spark 4 Upgrade Audit

## Build Update — DONE

`build.sbt` updated:
- ~~Scala `2.13.12`~~ -> `2.13.16`
- ~~Spark `3.5.0`~~ -> `4.0.0`
- JDK 21 target (JDK 8/11 dropped in Spark 4)
- Removed dead `bintray` resolver
- log4j updated to `2.24.3`

---

## New Lessons Added (Part 6 — Catalyst Optimizations)

### AdaptiveQueryExecution.scala — DONE

`part6catalyst/AdaptiveQueryExecution.scala` — covers all AQE features with runnable examples:
- **Coalescing shuffle partitions**: AQE merges small post-shuffle partitions automatically, eliminating the need to manually tune `spark.sql.shuffle.partitions`
- **Dynamic join strategy switching**: AQE converts SortMergeJoin to BroadcastHashJoin at runtime when one side is small enough
- **Skew join optimization**: AQE detects skewed partitions and splits them automatically, with comparison to manual explode technique
- **Empty relation detection**: AQE eliminates joins when one side is empty after filtering
- **When AQE is NOT enough**: pre-partitioned/pre-aggregated data where skew is baked in before the shuffle boundary
- **Reading AQE plans**: key markers (`AdaptiveSparkPlan`, `CustomShuffleReader`, `isFinalPlan`)

### DynamicPartitionPruning.scala — DONE

`part6catalyst/DynamicPartitionPruning.scala` — covers DPP with partitioned Parquet tables:
- **Static vs dynamic pruning**: literal filters vs runtime subquery filters
- **DPP in action**: partitioned fact table joined with filtered dimension table — only matching partitions scanned
- **Without DPP comparison**: disabling DPP to show full scan for contrast
- **Multi-key DPP** (Spark 4 improvement): compound partition keys with multi-layer runtime filtering
- **When DPP does not apply**: non-partitioned tables, non-equi joins, filters on the fact side

### PredicatePushdown.scala — DONE

`part6catalyst/PredicatePushdown.scala` — covers pushdown with existing course data (movies, flights, employees):
- **Parquet pushdown**: filters pushed into Parquet reader via column statistics (PushedFilters in plan)
- **CSV no-pushdown**: contrast showing CSV cannot leverage pushdown
- **Pushdown through joins**: Catalyst pushes filters into both sides of a join
- **UDFs block pushdown**: Scala UDFs are opaque to Catalyst; native expressions are not. Mentions Spark 4 SQL UDFs as the solution.
- **Filter ordering**: short-circuit evaluation and Catalyst predicate reordering
- **Combined pushdown + column pruning**: maximum I/O reduction when both techniques apply

### SqlUDFs.scala — DONE

`part6catalyst/SqlUDFs.scala` — covers the Spark 4 SQL UDF feature vs traditional Scala UDFs:
- **Scala UDF (black box)**: opaque to Catalyst, blocks pushdown and inlining
- **SQL UDF (inlined)**: Catalyst inlines the function body into the plan, enabling full optimization
- **Native expression baseline**: comparison showing SQL UDFs produce identical plans to native expressions
- **Complex SQL UDFs**: CASE WHEN, conditionals — still fully inlined
- **Composed SQL UDFs**: SQL UDFs calling other SQL UDFs, entire chain inlined
- **SQL UDFs with DataFrame API**: using `expr()` / `selectExpr()` to call SQL UDFs from Scala
- **When Scala UDFs are still needed**: external libraries, imperative logic, broadcast variables

---

## Remaining Changes to Existing Lessons

### Part 1 — Recap

**SparkRecap.scala** — ANSI mode is now the default (`spark.sql.ansi.enabled = true`). This is the single biggest behavioral change in Spark 4. Arithmetic overflow, invalid casts, division by zero, and array out-of-bounds now throw exceptions instead of returning null. Introduce the `try_*` function family (`try_cast`, `try_add`, `try_divide`, `try_to_timestamp`, etc.) as the new safety net. Good place to plant the seed since students will encounter this everywhere.

### Part 2 — Foundations

**SparkAPIs.scala** — The lesson correctly shows that lambdas/`Dataset.map()` are opaque to Catalyst. Spark 4 adds **SQL UDFs** (`CREATE FUNCTION ... RETURNS ... RETURN ...`) that the optimizer can inline and optimize. This is a natural extension of the "lambdas kill optimization" lesson: now there's a middle ground between raw SQL expressions and opaque Scala UDFs. Show the query plan difference between a Scala UDF, a SQL UDF, and a native expression.

**ReadingQueryPlans.scala** — No structural changes needed, but mention that Spark 4 plans may show new nodes like `StoragePartitionJoin` (no Exchange) and `VariantScan`. If AQE-related plan changes are shown (runtime broadcast conversion, partition coalescing), this is the right place.

**TestDeployApp.scala** — Update spark-submit flags and mention JDK 21 requirement. The `javax` to `jakarta` migration may affect custom Hadoop/Hive configurations.

### Part 3 — DataFrame Joins

**BroadcastJoins.scala** — Content is still correct, but add a section on how AQE can **auto-convert** sort-merge joins to broadcast joins at runtime if it discovers one side is small enough. Manual `broadcast()` hints are less critical than before. Show the plan difference: AQE's `AdaptiveSparkPlan` wrapping and the runtime decision. The `autoBroadcastJoinThreshold` config still applies.

**Bucketing.scala** — Lesson is correct for V1 DataSources (Hive-style tables). Add a section on **Storage Partition Join (SPJ)** for V2 DataSources (Iceberg, Delta). SPJ is the evolution of bucketing with new configs:
- `spark.sql.sources.v2.bucketing.enabled`
- `spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled`
- `spark.sql.sources.v2.bucketing.shuffle.enabled` (shuffle only one side)

Also mention `clusterBy()` on `DataFrameWriter` as the V2-native way to write bucketed data.

**SkewedJoins.scala** — The manual explode technique is still valid and educational, but AQE's built-in **skew join optimization** (`spark.sql.adaptive.skewJoin.enabled`, default true since 3.2) handles many of these cases automatically. Show both: the manual technique for understanding, then AQE's automatic handling, and when manual is still needed (e.g., AQE can't detect skew in pre-aggregated data).

**ColumnPruning.scala** — Add a note about **Dynamic Partition Pruning (DPP)** improvements in Spark 4: it now supports multiple filtering keys (compound partition keys) and multi-layer runtime filtering across multiple join levels. This is the runtime cousin of column pruning.

**PrePartitioning.scala** — No changes needed, the technique is timeless.

### Part 4 — RDD Joins

All four files are still correct. RDD APIs haven't changed significantly. Add a general note that RDD-level optimization is increasingly niche — most new Spark features (AQE, DPP, SPJ, SQL UDFs) only benefit the DataFrame/SQL API. This frames why the course teaches both but signals where the industry is heading.

### Part 5 — RDD Transformations

No changes needed. `aggregateByKey`, `reduceByKey`, `mapPartitions`, and object reuse are fundamentally unchanged. These are JVM-level optimization techniques that are version-independent.

---

## All new lessons complete.
