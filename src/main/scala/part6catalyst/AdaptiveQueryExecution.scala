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

  spark.sparkContext.setLogLevel("WARN")

  /**
   * AQE 1: Post-shuffle coalesce
   *
   * Without AQE, you set spark.sql.shuffle.partitions (default 200).
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

    println(s"Result partition count: ${grouped.rdd.getNumPartitions}")
    grouped.show()
  }

  /**
   * AQE 2: Automatic broadcast join
   *
   * Even if Spark initially plans a SortMergeJoin (because it can't estimate the filtered size),
   * AQE can switch to BroadcastHashJoin at runtime once it discovers one side is small enough.
   */
  def demoDynamicJoinStrategy(): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // disable static auto-broadcast
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", 10 * 1024 * 1024) // allow AQE auto-broadcast

    val largeTable = spark.range(1, 10000000).selectExpr("id as large_id", "id % 100 as key")
    val smallTable = spark.range(1, 100).selectExpr("id as small_id", "id as key")

    // Spark plans a SortMergeJoin at "compile" time (auto-broadcast is off)
    val joined = largeTable.join(smallTable, "key")
    joined.explain() // shows SortMergeJoin in the initial plan

    // after execution, AQE detects the small side is tiny and converts to BroadcastHashJoin
    // check the SQL tab in Spark UI: "AdaptiveSparkPlan" -> "BroadcastHashJoin"
    println(joined.count())
  }

  /**
   * AQE 3: Dynamically optimizing skew joins.
   *
   * AQE detects skewed partitions at runtime and splits them into smaller sub-partitions.
   * This is the automatic version of the manual explode technique from SkewedJoins.scala.
   */
  def demoSkewJoinOptimization(): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // force SortMergeJoin
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true") // default true
    // a partition is considered skewed if it's 5x the median AND > 100MB
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100m")

    // create skewed data: 80% of rows have key = 0
    val skewedTable = spark.range(1, 1000000000)
      .selectExpr("CASE WHEN id < 800000000 THEN 0 ELSE id % 100 END as key", "id as value1")

    val uniformTable = spark.range(1, 100)
      .selectExpr("id as key", "id as value2")

    val joined = skewedTable.join(uniformTable, "key")
    joined.explain() // initial plan: SortMergeJoin

    // AQE detects key=0 partition is skewed and splits it
    // in Spark UI: look for table metrics in the long stage - instead of ONE task taking the whole time, you'll see the median and max much closer
    println(joined.count())
  }

  def main(args: Array[String]): Unit = {
    demoCoalescingPartitions()
    demoDynamicJoinStrategy()
    demoSkewJoinOptimization()
    Thread.sleep(1000000)
  }
}
