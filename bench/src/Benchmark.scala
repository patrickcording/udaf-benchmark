import method._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalameter._

object Benchmark extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("UDAF Benchmark")
    .config("spark.sql.codegen.wholeStage", false)
//    .config("spark.executor.memory", "4g")
//    .config("spark.driver.memory", "4g")
    .getOrCreate()

  println("Generating dataframe...")
  val df = spark.range(1000000000)
    .toDF("salary")
    .withColumn("foo", col("salary").cast("string"))
    .withColumn("bar", concat(col("foo"), col("foo")))

  val methods = List(
    new AggregatorMethod,
    new AggregatorUdafMethod,
    new RddApiMethod,
    new CustomUdafMethod,
  )

  println("Writing dataframe...")
  df.write
    .option("compression", "uncompressed")
    .mode("overwrite")
    .parquet("tmp/data")

  println("Reading data...")
  val df2 = spark.read.parquet("tmp/data")

  val timer = config(Key.exec.benchRuns -> 3, Key.verbose -> false)
//    .withWarmer { new Warmer.Default }
//    .withMeasurer { new Measurer.IgnoringGC }

  Thread.sleep(30000)

  println("Running aggregators...")
//  val results = methods.map(method => method.run(df))
  val timings = methods.map(method => timer.measure(method.run(df2)).value)

//  println(results)
  println(timings)

  println("Keep Spark UI open")
  Thread.sleep(1000000)
}