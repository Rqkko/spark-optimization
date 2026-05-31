package part6catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AdaptiveQueryExecution {

  val spark = SparkSession.builder()
    .appName("Adaptive Query Execution")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true") // default true since Spark 3.2
    .getOrCreate()

  import spark.implicits._

  /**
   * AQE Feature 1: Dynamically coalescing shuffle partitions.
   *
   * Without AQE, you set spark.sql.shuffle.partitions (default 200) and live with it.
   * With AQE, Spark merges small post-shuffle partitions at runtime.
   */
  def demoCoalescingPartitions(): Unit = {
    val numbers = spark.range(1, 1000000)
    val grouped = numbers.selectExpr("id % 20 as key", "id as value")
      .groupBy("key")
      .agg(sum("value").as("total"))

    // AQE will coalesce the 200 default shuffle partitions into fewer, since most are tiny
    grouped.explain(true)
    // look for "AdaptiveSparkPlan isFinalPlan=false" in the plan
    // after execution, look for "CoalescedShuffleReader" in the Spark UI

    println(s"Result partition count: ${grouped.rdd.getNumPartitions}")
    grouped.show()

    // without AQE, you'd have to manually guess the right value:
    // spark.conf.set("spark.sql.shuffle.partitions", 20)
    // with AQE, this tuning is automatic
  }

  /**
   * AQE Feature 2: Dynamically switching join strategies.
   *
   * Even if Spark initially plans a SortMergeJoin (because it can't estimate the filtered size),
   * AQE can switch to BroadcastHashJoin at runtime once it discovers one side is small enough.
   */
  def demoDynamicJoinStrategy(): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // disable static auto-broadcast

    val largeTable = spark.range(1, 10000000).selectExpr("id as large_id", "id % 100 as key")
    val smallTable = spark.range(1, 100).selectExpr("id as small_id", "id as key")

    // Spark plans a SortMergeJoin at compile time (auto-broadcast is off)
    val joined = largeTable.join(smallTable, "key")
    joined.explain() // shows SortMergeJoin in the initial plan

    // but after execution, AQE detects the small side is tiny and converts to BroadcastHashJoin
    // check the SQL tab in Spark UI: "AdaptiveSparkPlan" -> "BroadcastHashJoin"
    println(joined.count())

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760) // restore default (10MB)
  }

  /**
   * AQE Feature 3: Dynamically optimizing skew joins.
   *
   * AQE detects skewed partitions at runtime and splits them into smaller sub-partitions.
   * This is the automatic version of the manual explode technique from SkewedJoins.scala.
   */
  def demoSkewJoinOptimization(): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // force SortMergeJoin
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true") // default true
    // a partition is considered skewed if it's 5x the median AND > 256MB
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

    // create skewed data: 80% of rows have key = 0
    val skewedTable = spark.range(1, 1000000)
      .selectExpr("CASE WHEN id < 800000 THEN 0 ELSE id % 100 END as key", "id as value1")

    val uniformTable = spark.range(1, 100)
      .selectExpr("id as key", "id as value2")

    val joined = skewedTable.join(uniformTable, "key")
    joined.explain() // initial plan: SortMergeJoin

    // AQE detects key=0 partition is skewed and splits it
    // in Spark UI: look for "CustomShuffleReader" with "skewed" partitions
    println(joined.count())

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
  }

  /**
   * AQE Feature 4: Dynamically detecting and propagating empty relations.
   *
   * If AQE discovers at runtime that one side of a join is empty (e.g., after filtering),
   * it can eliminate the join entirely.
   */
  def demoEmptyRelationOptimization(): Unit = {
    val table1 = spark.range(1, 1000000).selectExpr("id as key", "id as value1")
    val table2 = spark.range(1, 1000000).selectExpr("id as key", "id as value2")

    // this filter eliminates ALL rows at runtime (no id is negative)
    val emptyTable = table2.filter($"key" < 0)

    val joined = table1.join(emptyTable, "key")
    joined.explain() // Spark may still show a join in the initial plan
    println(joined.count()) // AQE detects empty relation -> shortcircuits to 0
  }

  /**
   * When AQE is NOT enough: pre-aggregated or pre-joined data.
   *
   * AQE detects skew at shuffle boundaries. If your data is already skewed BEFORE a shuffle
   * (e.g., reading a pre-partitioned Parquet file), AQE won't see it.
   * In these cases, manual techniques (explode, salting) are still necessary.
   */
  def whenAQEIsNotEnough(): Unit = {
    // simulate pre-partitioned skewed data
    val skewedData = (Seq.fill(800000)((0, "skewed")) ++ (1 to 200000).map(i => (i % 100, "normal"))).toDF("key", "label")

    // repartition on key BEFORE the join — AQE sees the partitions post-shuffle,
    // but the skew was baked in by the repartition itself
    val prePartitioned = skewedData.repartition(200, $"key")

    val lookup = spark.range(0, 100).selectExpr("id as key", "id as value")

    // AQE will try, but the damage is already done in the repartition step
    val joined = prePartitioned.join(lookup, "key")
    joined.explain()
    println(joined.count())
  }

  /**
   * Reading AQE query plans.
   *
   * Key markers in AQE plans:
   * - "AdaptiveSparkPlan isFinalPlan=false" -> plan will be re-optimized at runtime
   * - "AdaptiveSparkPlan isFinalPlan=true"  -> final plan after AQE optimizations
   * - "CustomShuffleReader"                 -> AQE coalesced or split partitions
   * - "BroadcastHashJoin" replacing         -> AQE switched strategy from SortMergeJoin
   */
  def readingAQEPlans(): Unit = {
    val table1 = spark.range(1, 10000000).selectExpr("id % 50 as key", "id as value")
    val table2 = spark.range(1, 100).selectExpr("id as key", "id as label")

    val joined = table1.join(table2, "key")

    // before execution
    println("=== Initial plan (isFinalPlan=false) ===")
    joined.explain()

    // trigger execution
    joined.count()

    // after execution, the plan in Spark UI's SQL tab shows the final plan
    // with AQE optimizations applied
    println("=== Check Spark UI SQL tab for the final AQE plan ===")
  }

  def main(args: Array[String]): Unit = {
    demoCoalescingPartitions()
    demoDynamicJoinStrategy()
    demoSkewJoinOptimization()
    demoEmptyRelationOptimization()
    whenAQEIsNotEnough()
    readingAQEPlans()
    Thread.sleep(1000000)
  }
}
