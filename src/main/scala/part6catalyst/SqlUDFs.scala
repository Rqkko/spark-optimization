package part6catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SqlUDFs {

  val spark = SparkSession.builder()
    .appName("SQL UDFs vs Scala UDFs")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  /**
   * The problem with Scala/Python UDFs:
   *
   * As shown in SparkAPIs.scala, lambdas and UDFs are opaque to Catalyst.
   * Spark can't inline, reorder, or push them down — they are black boxes.
   *
   * Spark 4 introduces SQL UDFs: user-defined functions written in pure SQL.
   * Catalyst CAN inline these into the query plan and optimize through them.
   */

  // ─── Setup ────────────────────────────────────────────────────────────

  val movies = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies/movies.json")

  // ─── Scenario 1: Scala UDF ─────────────────────────────────────────────

  /**
   * A Scala UDF is a black box to Catalyst.
   * Spark must deserialize each row, apply the function, and serialize back.
   * No predicate pushdown, no constant folding, no inlining.
   */
  def scalaUdfExample(): Unit = {
    val profitUdf = udf((gross: java.lang.Long, budget: java.lang.Long) => {
      if (gross == null || budget == null) null
      else java.lang.Long.valueOf(gross - budget)
    })

    val withProfit = movies
      .withColumn("profit", profitUdf($"Worldwide_Gross", $"Production_Budget"))
      .filter($"profit" > 100000000)

    println("=== Scala UDF: opaque to Catalyst ===")
    withProfit.explain(true)
    // the UDF appears as a black box in the plan
    // no PushedFilters for the profit condition
    // Spark can't push "profit > 100M" into the data source

    withProfit.select("Title", "profit").show(5, truncate = false)
  }

  // ─── Scenario 2: SQL UDF (Spark 4) ───────────────────────────────────

  /**
   * A SQL UDF is written in pure SQL. Catalyst can inline it into the plan,
   * optimize through it, and push predicates past it.
   */
  def sqlUdfExample(): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION profit(gross BIGINT, budget BIGINT)
        |RETURNS BIGINT
        |RETURN gross - budget
      """.stripMargin)

    movies.createOrReplaceTempView("movies")

    val withProfit = spark.sql(
      """
        |SELECT Title, profit(Worldwide_Gross, Production_Budget) as profit
        |FROM movies
        |WHERE profit(Worldwide_Gross, Production_Budget) > 100000000
      """.stripMargin)

    println("=== SQL UDF: inlined by Catalyst ===")
    withProfit.explain(true)
    // the SQL UDF is INLINED: you'll see (Worldwide_Gross - Production_Budget) directly in the plan
    // no opaque UDF node — Catalyst treats it as a native expression
    // predicate pushdown and other optimizations can work through it

    withProfit.show(5, truncate = false)
  }

  // ─── Scenario 3: Native Column expressions (baseline) ─────────────────

  /**
   * Native Column expressions are always the fastest — no UDF overhead at all.
   * This is the baseline to compare against.
   */
  def nativeExpressionExample(): Unit = {
    val withProfit = movies
      .withColumn("profit", $"Worldwide_Gross" - $"Production_Budget")
      .filter($"profit" > 100000000)

    println("=== Native expression: fully optimized ===")
    withProfit.explain(true)
    // identical plan to the SQL UDF version — both are fully inlined

    withProfit.select("Title", "profit").show(5, truncate = false)
  }

  // ─── Scenario 4: SQL UDF with complex logic ────────────────────────────

  /**
   * SQL UDFs support conditionals, CASE WHEN, nested function calls, etc.
   * As long as the logic is expressible in SQL, Catalyst can optimize it.
   */
  def complexSqlUdf(): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION movie_tier(budget BIGINT, gross BIGINT)
        |RETURNS STRING
        |RETURN CASE
        |  WHEN gross IS NULL OR budget IS NULL THEN 'Unknown'
        |  WHEN gross > budget * 3 THEN 'Blockbuster'
        |  WHEN gross > budget THEN 'Profitable'
        |  ELSE 'Flop'
        |END
      """.stripMargin)

    movies.createOrReplaceTempView("movies")

    val tiered = spark.sql(
      """
        |SELECT Title, Production_Budget, Worldwide_Gross,
        |       movie_tier(Production_Budget, Worldwide_Gross) as tier
        |FROM movies
        |WHERE movie_tier(Production_Budget, Worldwide_Gross) = 'Blockbuster'
      """.stripMargin)

    println("=== Complex SQL UDF: still inlined ===")
    tiered.explain(true)
    // CASE WHEN is inlined into the plan — no black-box UDF node

    tiered.show(10, truncate = false)
  }

  // ─── Scenario 5: SQL UDF composition ───────────────────────────────────

  /**
   * SQL UDFs can call other SQL UDFs. Catalyst inlines the entire chain.
   */
  def composedSqlUdfs(): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION profit(gross BIGINT, budget BIGINT)
        |RETURNS BIGINT
        |RETURN gross - budget
      """.stripMargin)

    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION roi(gross BIGINT, budget BIGINT)
        |RETURNS DOUBLE
        |RETURN CAST(profit(gross, budget) AS DOUBLE) / CAST(budget AS DOUBLE) * 100
      """.stripMargin)

    movies.createOrReplaceTempView("movies")

    val highRoi = spark.sql(
      """
        |SELECT Title, Production_Budget, Worldwide_Gross,
        |       roi(Worldwide_Gross, Production_Budget) as roi_pct
        |FROM movies
        |WHERE Production_Budget > 0 AND roi(Worldwide_Gross, Production_Budget) > 500
      """.stripMargin)

    println("=== Composed SQL UDFs: fully inlined chain ===")
    highRoi.explain(true)
    // both profit() and roi() are inlined — the plan shows raw arithmetic

    highRoi.show(10, truncate = false)
  }

  // ─── Scenario 6: SQL UDFs with DataFrame API ──────────────────────────

  /**
   * SQL UDFs registered via spark.sql can also be called from the DataFrame API
   * using expr() or selectExpr().
   */
  def sqlUdfWithDataFrameApi(): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION profit(gross BIGINT, budget BIGINT)
        |RETURNS BIGINT
        |RETURN gross - budget
      """.stripMargin)

    val withProfit = movies
      .withColumn("profit", expr("profit(Worldwide_Gross, Production_Budget)"))
      .filter(expr("profit(Worldwide_Gross, Production_Budget) > 100000000"))

    println("=== SQL UDF called from DataFrame API via expr() ===")
    withProfit.explain(true)
    // same inlining as the pure SQL version

    withProfit.select("Title", "profit").show(5, truncate = false)
  }

  // ─── Scenario 7: When you still need Scala UDFs ───────────────────────

  /**
   * SQL UDFs can only contain SQL expressions. You still need Scala UDFs for:
   * - External library calls (regex engines, ML models, HTTP clients)
   * - Complex imperative logic that can't be expressed in SQL
   * - Accessing Spark internals or broadcast variables
   *
   * When forced to use a Scala UDF, mark it as DETERMINISTIC if it is — this allows
   * Spark to cache results and avoid redundant evaluations.
   */
  def whenScalaUdfIsNeeded(): Unit = {
    // example: a regex-based extraction that would be awkward in pure SQL
    val extractYear = udf((title: String) => {
      if (title == null) null
      else {
        val pattern = """\((\d{4})\)""".r
        pattern.findFirstMatchIn(title).map(_.group(1)).orNull
      }
    })

    // for deterministic UDFs, Spark can optimize repeated evaluations
    val deterministicExtractYear = udf((title: String) => {
      if (title == null) null
      else {
        val pattern = """\((\d{4})\)""".r
        pattern.findFirstMatchIn(title).map(_.group(1)).orNull
      }
    }).asNondeterministic() // call this ONLY if the UDF is truly nondeterministic
    // by default, UDFs are deterministic — Spark can avoid recomputing them

    val result = movies.withColumn("year_from_title", extractYear($"Title"))
    println("=== Scala UDF: still needed for complex logic ===")
    result.explain(true)
    result.select("Title", "year_from_title").show(5, truncate = false)
  }

  /**
   * LESSON SUMMARY:
   *
   * 1. Native Column expressions > SQL UDFs > Scala/Python UDFs (in optimization potential).
   * 2. SQL UDFs are inlined by Catalyst — same plan as native expressions.
   * 3. Scala UDFs are black boxes — block pushdown, prevent inlining.
   * 4. Use SQL UDFs for reusable logic that IS expressible in SQL.
   * 5. Reserve Scala UDFs for logic that REQUIRES JVM code (external libs, imperative logic).
   * 6. SQL UDFs compose: Catalyst inlines the entire call chain.
   * 7. SQL UDFs work from both SQL and DataFrame API (via expr()).
   */

  def main(args: Array[String]): Unit = {
    scalaUdfExample()
    sqlUdfExample()
    nativeExpressionExample()
    complexSqlUdf()
    composedSqlUdfs()
    sqlUdfWithDataFrameApi()
    whenScalaUdfIsNeeded()
    Thread.sleep(1000000)
  }
}
