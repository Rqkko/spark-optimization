package part6catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PredicatePushdown {

  val spark = SparkSession.builder()
    .appName("Predicate Pushdown")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def setupData(): Unit = {
    val movies = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies/movies.json")

    movies.write.mode("overwrite").parquet("src/main/resources/data/movies/movies.parquet")

    val flights = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights/flights.json")

    flights.write.mode("overwrite").parquet("src/main/resources/data/flights/flights.parquet")
  }

  /**
   * Predicate pushdown = Spark pushes filter conditions down into the data source,
   * so rows are eliminated BEFORE they enter the Spark execution engine.
   *
   * For Parquet/ORC: filters are applied at the row group level using column statistics
   * (min/max values, null counts). Entire row groups can be skipped without reading them.
   *
   * For CSV/JSON: no pushdown is possible — every row must be read and parsed first.
   */

  /**
   * Scenario 1: Pushdown into Parquet.
   * Parquet stores column min/max stats per row group, allowing Spark to skip irrelevant groups.
   */
  def parquetPushdown(): Unit = {
    val movies = spark.read.parquet("src/main/resources/data/movies/movies.parquet")

    val highBudget = movies.filter($"Production_Budget" > 200000000)
    println("=== Parquet: filter pushdown ===")
    highBudget.explain()
    // PushedFilters: [IsNotNull(Production_Budget), GreaterThan(Production_Budget, 200000000)]
    // the filter is pushed INTO the Parquet reader

    highBudget.select("Title", "Production_Budget").show(5, truncate = false)
  }

  /**
   * Scenario 2: No pushdown for CSV.
   * CSV has no column statistics — Spark must read every row and filter AFTER parsing.
   */
  def csvNoPushdown(): Unit = {
    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/employees_headers/employees_headers.csv")

    val filtered = employees.filter($"salary" > 50000)
    println("=== CSV: no filter pushdown ===")
    filtered.explain()
    // PushedFilters: [] — empty! CSV reader can't use filters
    // the Filter node appears ABOVE the FileScan

    filtered.show()
  }

  /**
   * Scenario 3: Pushdown through joins.
   *
   * Catalyst pushes filters as early as possible in the plan.
   * A filter on a column from ONE side of a join is pushed before the join.
   */
  def pushdownThroughJoins(): Unit = {
    val flights = spark.read.parquet("src/main/resources/data/flights/flights.parquet")

    val origins = flights.select("origin", "carrier").distinct()
    val details = flights.select("origin", "dest", "depdelay", "dist")

    // filter on origin — this should push down BEFORE the join, into BOTH sides
    val result = origins.join(details, "origin")
      .filter($"origin" === "ATL")

    println("=== Pushdown through join ===")
    result.explain()
    // the filter "origin = ATL" is pushed into BOTH sides of the join
    // look for PushedFilters on both FileScan nodes
  }

  /**
   * Scenario 4: UDFs block pushdown.
   *
   * Spark can't push opaque Scala/Python UDFs into the data source,
   * because it can't reason about what the function does.
   */
  def udfBlocksPushdown(): Unit = {
    val movies = spark.read.parquet("src/main/resources/data/movies/movies.parquet")

    val isExpensive = udf((budget: java.lang.Long) => budget != null && budget > 200000000L)

    val expensive = movies.filter(isExpensive($"Production_Budget"))
    println("=== UDF blocks pushdown ===")
    expensive.explain()
    // PushedFilters: [] — the UDF is opaque, nothing is pushed
    // compare with parquetPushdown() above where the same filter IS pushed

    // LESSON: prefer native Column expressions over UDFs when possible
    // the equivalent native expression:
    val expensiveNative = movies.filter($"Production_Budget" > 200000000)
    println("=== Native expression: pushdown works ===")
    expensiveNative.explain()
    // PushedFilters: [GreaterThan(Production_Budget, 200000000)]
  }

  /**
   * Scenario 5: Filter ordering for short-circuit evaluation.
   *
   * Catalyst reorders predicates, but when two filters have very different selectivity,
   * placing the more selective filter first helps Spark skip work earlier.
   *
   * In practice, Catalyst is smart about this for simple predicates. But with complex
   * expressions (especially ones involving UDFs), manual ordering can help.
   */
  def filterOrdering(): Unit = {
    val flights = spark.read.parquet("src/main/resources/data/flights/flights.parquet")

    // selective filter first (very few flights > 4000 miles), then broad filter
    val approach1 = flights.filter($"dist" > 4000).filter($"carrier" === "AA")
    // broad filter first, then selective
    val approach2 = flights.filter($"carrier" === "AA").filter($"dist" > 4000)

    println("=== Approach 1: selective first ===")
    approach1.explain()
    println("=== Approach 2: broad first ===")
    approach2.explain()
    // Catalyst combines them into a single Filter with both predicates
    // for Parquet pushdown, both are pushed regardless of order
    // but for UDF-based filters or non-pushdown sources, order matters

    println(s"Approach 1 count: ${approach1.count()}")
    println(s"Approach 2 count: ${approach2.count()}")
  }

  /**
   * Scenario 6: Pushdown with column pruning (combined effect).
   *
   * Column pruning (ReadSchema) and predicate pushdown (PushedFilters) work together:
   * - Column pruning: only read the columns you need
   * - Predicate pushdown: only read the row groups that match
   * Together they minimize I/O.
   */
  def combinedPushdownAndPruning(): Unit = {
    val movies = spark.read.parquet("src/main/resources/data/movies/movies.parquet")

    // select only 2 columns, filter on a third
    val result = movies
      .filter($"Production_Budget" > 100000000)
      .select("Title", "Worldwide_Gross")

    println("=== Combined pushdown + column pruning ===")
    result.explain()
    // ReadSchema: struct<Title:string,Worldwide_Gross:long> — only 2 columns read (+ Production_Budget for filter)
    // PushedFilters: [GreaterThan(Production_Budget, 100000000)]

    result.show(5, truncate = false)
  }

  /**
   * LESSON SUMMARY:
   *
   * 1. Use Parquet/ORC over CSV/JSON — they support pushdown via column statistics.
   * 2. Use native Column expressions over UDFs — UDFs block pushdown entirely.
   * 3. Combine predicate pushdown with column pruning for maximum I/O reduction.
   * 4. Pushdown works THROUGH joins — Catalyst pushes filters into both sides.
   * 5. For Spark 4 SQL UDFs (CREATE FUNCTION ... RETURN ...): Catalyst CAN inline these,
   *    so pushdown works through SQL UDFs, unlike Scala/Python UDFs.
   */

  def main(args: Array[String]): Unit = {
    setupData()
    parquetPushdown()
    csvNoPushdown()
    pushdownThroughJoins()
    udfBlocksPushdown()
    filterOrdering()
    combinedPushdownAndPruning()
    Thread.sleep(1000000)
  }
}
