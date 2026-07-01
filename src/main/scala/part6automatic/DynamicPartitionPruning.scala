package part6automatic

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions._

object DynamicPartitionPruning {

  val spark = SparkSession.builder()
    .appName("Dynamic Partition Pruning")
    .master("local[*]")
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") // default true
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  def setupTables() = {
    val salesRegions = spark.range(1, 2000000)
      .selectExpr(
        "id as sale_id",
        "CAST(id % 10 AS INT) as region_id",
        "CAST(id % 100 AS INT) as product_id",
        "CAST(rand() * 1000 AS DECIMAL(10, 2)) as amount"
      )

    val sales = spark.range(1, 5000000)
      .selectExpr(
        "id as sale_id",
        "CAST(id % 100 AS INT) as product_id",
        "CAST(id % 365 AS INT) as day_of_year",
        "CAST(rand() * 1000 AS DECIMAL(10, 2)) as amount"
      )

    salesRegions.write.partitionBy("region_id", "product_id").parquet("src/main/resources/data/dpp/sales_regions")
    sales.write.partitionBy("product_id").parquet("src/main/resources/data/dpp/sales")

    val products = (0 until 100).map { i =>
      (
        i,
        s"Product_$i",
        if (i < 10) "Electronics" else if (i < 30) "Clothing" else "Other"
      )
    }.toDF("product_id", "product_name", "category")

    val regionProducts = for {
      regionId <- 0 until 10
      productId <- 0 until 100
    } yield {
      val regionName = regionId match {
        case 0 => "US"
        case 1 => "EU"
        case 2 => "APAC"
        case 3 => "LATAM"
        case _ => "OTHER"
      }

      val productTier = if (regionId == 3 && productId < 5) "featured" else "standard"
      (regionId, productId, regionName, productTier)
    }

    regionProducts.toDF("region_id", "product_id", "region_name", "product_tier").write.parquet("src/main/resources/data/dpp/region_products")

    products.write.parquet("src/main/resources/data/dpp/products")
  }

  // static partition pruning
  def staticPruning() = {
    val sales = spark.read.parquet("src/main/resources/data/dpp/sales") // already partitioned DF
    val filtered = sales.filter(col("product_id") === 5)
    filtered.explain()

    println(s"Filtered count: ${filtered.count()}")
  }

  // dynamic partition pruning
  def dynamicPruning() = {
    val sales = spark.read.parquet("src/main/resources/data/dpp/sales")
    val products = spark.read.parquet("src/main/resources/data/dpp/products")

    val electronics = sales.join(products, "product_id")
      .filter(col("category") === "Electronics")
      .groupBy("product_name")
      .agg(sum("amount").as("total_revenue"))

    electronics.explain()
    electronics.show()
  }

  // multi-key DPP
  def multiKeyDPP() = {
    val salesRegions = spark.read.parquet("src/main/resources/data/dpp/sales_regions")
    val regionProducts = spark.read.parquet("src/main/resources/data/dpp/region_products")

    val featuredRegionRevenue = salesRegions.join(regionProducts, List("region_id", "product_id"))
      .filter(col("region_name") === "LATAM" && col("product_tier") === "featured")
      .agg(sum("amount").as("total"))

    featuredRegionRevenue.explain()
    featuredRegionRevenue.show()
  }

  /*
    When DPP does NOT apply
    - if the table(s) are not partitioned
    - for non-equi-joins
    * if the scan for partitions is too big an overhead
   */

  def main(args: Array[String]): Unit = {
    multiKeyDPP()
    Thread.sleep(99999)
  }
}
