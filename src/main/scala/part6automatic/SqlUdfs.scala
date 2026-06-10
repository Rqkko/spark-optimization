package part6automatic

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions._

object SqlUdfs {

  val spark = SparkSession.builder()
    .appName("SQL UDFs")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val movies = spark.read.json("src/main/resources/data/movies")

  def sqlUdfExample() = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION movie_tier(budget BIGINT, gross BIGINT)
        |RETURNS STRING
        |RETURN CASE
        |   WHEN gross IS NULL OR budget IS NULL THEN 'unknown'
        |   WHEN gross > budget * 2 THEN 'blockbuster'
        |   WHEN gross > budget THEN 'profitable'
        |   ELSE 'flop'
        |END
        |""".stripMargin
    )

    movies.createOrReplaceTempView("movies")

    val tiered = spark.sql(
      """
        |SELECT Title, Worldwide_Gross, Production_Budget,
        |   movie_tier(Production_Budget, Worldwide_Gross) as Tier
        |FROM movies
        |WHERE movie_tier(Production_Budget, Worldwide_Gross) = 'blockbuster'
        |""".stripMargin
    )

    tiered.explain()
    tiered.show()
  }

  def scalaUdfExample() = {
    val movieTier = udf((budget: java.lang.Long, gross: java.lang.Long) => {
      if (gross == null || budget == null) "unknown"
      else if (gross > budget * 2) "blockbuster"
      else if (gross > budget) "profitable"
      else "flop"
    })

    val tiered = movies
      .withColumn("Tier", movieTier(col("Production_Budget"), col("Worldwide_Gross")))
      .filter(col("Tier") === "blockbuster")
      .select("Title", "Production_Budget", "Worldwide_Gross", "Tier")

    tiered.explain()
    tiered.show()
  }

  def composedSqlUdfs() = {
    movies.createOrReplaceTempView("movies")

    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION profit(gross BIGINT, budget BIGINT)
        |RETURNS BIGINT
        |RETURN gross - budget
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY FUNCTION roi(gross BIGINT, budget BIGINT)
        |RETURNS DOUBLE
        |RETURN CAST(profit(gross, budget) AS DOUBLE) / CAST(budget AS DOUBLE) * 100
        |""".stripMargin
    )

    val highRoi = spark.sql(
      """
        |SELECT Title, Production_Budget, Worldwide_Gross,
        | roi(Worldwide_Gross, Production_Budget) AS Roi
        |FROM movies
        |WHERE Production_Budget > 0 AND roi(Worldwide_Gross, Production_Budget) > 10
        |""".stripMargin
    )

    highRoi.explain()
    highRoi.show()
  }

  // note - SQL UDFs can be called from the DF API - with selectExpr

  def scalaUdfsNeeded() = {
    val extractYear = udf((title: String) => {
      if (title == null) null
      else {
        val pattern = """\((\d{4})\)""".r
        pattern.findFirstMatchIn(title).map(_.group(1)).orNull
      }
    })

    val result = movies.withColumn("Year_From_Title", extractYear(col("Title"))).filter(col("Year_From_Title").isNotNull)
    result.explain()
    result.show()
  }

  /*
    tips
    - native col functions > SQL UDFs > Scala UDF in optimization potential
    - SQL UDFs are inlined (by Catalyst) to native exprs whenever possible
    - Scala UDFs are black boxes - use these only if SQL/DF API cannot (or with difficulty) compute the same thing
    - don't be afraid to compose SQL UDF
    - can use SQL UDFs from the DF API via selectExpr
   */

  def main(args: Array[String]): Unit = {
    scalaUdfsNeeded()
  }
}
