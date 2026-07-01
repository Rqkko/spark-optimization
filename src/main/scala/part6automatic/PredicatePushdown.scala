package part6automatic

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions._

object PredicatePushdown {

  val spark = SparkSession.builder()
    .appName("Predicate Pushdown")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  def setupData() = {
    spark.read.json("src/main/resources/data/movies")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/movies_parquet")

    spark.read.json("src/main/resources/data/flights")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/flights_parquet")
  }

  def parquetPushdown() = {
    val movies = spark.read.parquet("src/main/resources/data/movies_parquet")
    val goodMovies = movies.filter(col("IMDB_Rating") > 7.0)

    goodMovies.explain()
    goodMovies.show()
  }

  def pushdownThroughJoins() = {
    val flights = spark.read.parquet("src/main/resources/data/flights_parquet")
    val origins = flights.select("origin", "carrier").distinct()
    val details = flights.select("origin", "dest", "depdelay", "dist")

    val result = origins.join(details, "origin")
      .filter(col("origin") === "ATL")

    result.explain()
  }

  // UDFs block pushdown
  def udfBlocksPushdown() = {
    val movies = spark.read.parquet("src/main/resources/data/movies_parquet")
    val isExpensive = udf((budget: java.lang.Long) => budget != null && budget > 100000000L)

    val expensive = movies.filter(isExpensive(col("Production_Budget")))
    expensive.explain()

    // if your udf is a filter, just run a filter on the col!
    val expensiveNative = movies.filter(col("Production_Budget") > 100000000)
    expensiveNative.explain()

    // good practice: run the most SELECTIVE filter FIRST

    val expensiveAndHighlyRated = movies.filter(col("Production_Budget") > 100000000 && col("IMDB_Rating") > 7.0)
    val expensiveAndHighlyRated_v2 = movies.filter(col("Production_Budget") > 100000000).filter(col("IMDB_Rating") > 7.0)
    val expensiveAndHighlyRated_v3 = movies.filter(col("IMDB_Rating") > 7.0).filter(col("Production_Budget") > 100000000)
    expensiveAndHighlyRated.explain()
    expensiveAndHighlyRated_v2.explain()
    expensiveAndHighlyRated_v3.explain()
  }

  // spark can combine PP with DPP and column pruning
  def combinedPushdownAndPruning() = {
    val movies = spark.read.parquet("src/main/resources/data/movies_parquet")

    val result = movies
      .filter(col("Production_Budget") > 100000000)
      .select("Title", "IMDB_Rating")

    result.explain()
  }

  /*
    - use Parquet over CSV/JSON - this supports predicate pushdown at the source
    - use native functions over UDFs that do a filter
    - combine PP with column pruning or DPP
    - PP works through ops like joins or aggs
    - SQL UDFs are preferable over Scala UDFs - Spark can inline the SQL UDFs and can analyze & push predicates down as much as possible
   */

  def main(args: Array[String]): Unit = {
    combinedPushdownAndPruning()
  }

}
