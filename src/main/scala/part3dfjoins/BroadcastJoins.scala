package part3dfjoins

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object BroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  val rows = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third")
  ))

  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  // small table
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)

  // large table
  val table = spark.range(1, 100000000) // column is "id"

  // the innocent join
  val joined = table.join(lookupTable, "id")
  joined.explain
  // joined.show - takes an ice age

  // a smarter join
  // Broadcast join: Smaller DF (lookupTable) is sent to all executors
  val joinedSmart = table.join(broadcast(lookupTable), "id")
  joinedSmart.explain()
  // joinedSmart.show()

  // auto-broadcast detection
  val bigTable = spark.range(1, 100000000)
  val smallTable = spark.range(1, 10000) // size estimated by Spark - auto-broadcast
  val joinedNumbers = smallTable.join(bigTable, "id")

  // deactivate auto-broadcast
  // Default: 10 MB
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // number in bytes (e.g. set to 30 above the statement, will not auto broadcast because smallTable is > 30 bytes)

  joinedNumbers.explain()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
