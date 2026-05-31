package part6catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DynamicPartitionPruning {

  val spark = SparkSession.builder()
    .appName("Dynamic Partition Pruning")
    .master("local[*]")
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") // default true since Spark 3.0
    .getOrCreate()

  import spark.implicits._

  /**
   * Static vs Dynamic partition pruning.
   *
   * Static: the filter value is a literal known at compile time.
   *   -> Spark pushes it directly into the scan (PushedFilters in plan).
   *
   * Dynamic: the filter value comes from the OTHER side of a join, only known at runtime.
   *   -> Spark injects a "DynamicPruningExpression" subquery at runtime.
   */

  // write a partitioned fact table
  def setupTables(): Unit = {
    val sales = spark.range(1, 5000000)
      .selectExpr(
        "id as sale_id",
        "CAST(id % 100 AS INT) as product_id",
        "CAST(id % 365 AS INT) as day_of_year",
        "CAST(rand() * 1000 AS DECIMAL(10,2)) as amount"
      )

    sales.write.mode("overwrite").partitionBy("product_id").parquet("src/main/resources/data/dpp/sales")

    val products = (0 until 100).map(i => (i, s"Product_$i", if (i < 10) "Electronics" else if (i < 30) "Clothing" else "Other"))
      .toDF("product_id", "product_name", "category")

    products.write.mode("overwrite").parquet("src/main/resources/data/dpp/products")
  }

  /**
   * Scenario 1: Static partition pruning.
   * The filter is a literal — Spark knows at compile time which partitions to read.
   */
  def staticPruning(): Unit = {
    val sales = spark.read.parquet("src/main/resources/data/dpp/sales")

    val filtered = sales.filter($"product_id" === 5)
    filtered.explain()
    // PartitionFilters: [isnotnull(product_id), (product_id = 5)]
    // only 1 partition directory is read

    println(s"Filtered count: ${filtered.count()}")
  }

  /**
   * Scenario 2: Dynamic partition pruning (DPP).
   *
   * The fact table (sales) is partitioned by product_id.
   * We join with a dimension table (products) and filter on the dimension side.
   * The filter value is NOT a literal — it comes from the join at runtime.
   *
   * WITHOUT DPP: Spark scans ALL 100 product_id partitions, then filters after the join.
   * WITH DPP: Spark first evaluates the dimension filter, collects matching product_ids,
   *           and prunes the fact table partitions BEFORE scanning.
   */
  def dynamicPruning(): Unit = {
    val sales = spark.read.parquet("src/main/resources/data/dpp/sales")
    val products = spark.read.parquet("src/main/resources/data/dpp/products")

    val electronicsRevenue = sales.join(products, "product_id")
      .filter($"category" === "Electronics") // filter on the DIMENSION table
      .groupBy("product_name")
      .agg(sum("amount").as("total_revenue"))

    println("=== WITH DPP (default) ===")
    electronicsRevenue.explain()
    // look for "DynamicPruningExpression" in the plan — means DPP injected a subquery
    // PartitionFilters: [isnotnull(product_id), dynamicpruningexpression(product_id IN subquery)]
    // only 10 out of 100 partitions are read (Electronics = product_id 0-9)
    electronicsRevenue.show()
  }

  /**
   * Scenario 3: Without DPP — full scan.
   */
  def withoutDPP(): Unit = {
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")

    val sales = spark.read.parquet("src/main/resources/data/dpp/sales")
    val products = spark.read.parquet("src/main/resources/data/dpp/products")

    val electronicsRevenue = sales.join(products, "product_id")
      .filter($"category" === "Electronics")
      .groupBy("product_name")
      .agg(sum("amount").as("total_revenue"))

    println("=== WITHOUT DPP ===")
    electronicsRevenue.explain()
    // no DynamicPruningExpression — all 100 partitions are scanned
    electronicsRevenue.show()

    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
  }

  /**
   * Scenario 4: Multi-key DPP (improved in Spark 4).
   *
   * Spark 4 can broadcast multiple filtering keys for DPP,
   * so joins against compound-partition fact tables prune at runtime.
   */
  def multiKeyDPP(): Unit = {
    // fact table partitioned by TWO columns
    val sales = spark.range(1, 2000000)
      .selectExpr(
        "id as sale_id",
        "CAST(id % 10 AS INT) as region_id",
        "CAST(id % 50 AS INT) as product_id",
        "CAST(rand() * 1000 AS DECIMAL(10,2)) as amount"
      )
    sales.write.mode("overwrite").partitionBy("region_id", "product_id").parquet("src/main/resources/data/dpp/sales_multi")

    val regions = Seq((0, "US"), (1, "EU"), (2, "APAC"), (3, "LATAM"))
      .toDF("region_id", "region_name")

    val multiPartSales = spark.read.parquet("src/main/resources/data/dpp/sales_multi")

    val usRevenue = multiPartSales.join(regions, "region_id")
      .filter($"region_name" === "US")
      .agg(sum("amount").as("total"))

    println("=== Multi-key DPP ===")
    usRevenue.explain()
    // DPP prunes on region_id — only 1/10 of top-level partitions scanned
    usRevenue.show()
  }

  /**
   * When DPP does NOT apply:
   * - The fact table is not partitioned
   * - The filter is on the fact table side (not the dimension)
   * - The join is not an equi-join
   * - The dimension side is too large (Spark won't broadcast the subquery)
   */
  def whenDPPDoesNotApply(): Unit = {
    // non-partitioned table — DPP has nothing to prune
    val salesFlat = spark.range(1, 1000000)
      .selectExpr("id as sale_id", "CAST(id % 100 AS INT) as product_id", "CAST(rand() * 1000 AS DECIMAL(10,2)) as amount")

    val products = spark.read.parquet("src/main/resources/data/dpp/products")

    val result = salesFlat.join(products, "product_id")
      .filter($"category" === "Electronics")
      .agg(sum("amount").as("total"))

    println("=== No DPP (fact table not partitioned) ===")
    result.explain()
    // no DynamicPruningExpression — fact table has no partitions to prune
    result.show()
  }

  def main(args: Array[String]): Unit = {
    setupTables()
    staticPruning()
    dynamicPruning()
    withoutDPP()
    multiKeyDPP()
    whenDPPDoesNotApply()
    Thread.sleep(1000000)
  }
}
