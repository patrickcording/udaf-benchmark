import method.{AggregatorMethod, AggregatorUdafMethod, CustomUdafMethod, MapPartitionsMethod, RddApiMethod}
import org.apache.spark.sql.SparkSession
import org.scalameter._

import scala.io.Source
import scala.util.Random

object Benchmark extends App {

  val spark = SparkSession.builder
    .master("local[4]")
    .appName("UDAF Benchmark")
    .getOrCreate()
  import spark.implicits._

  // Load data
  println("Loading dataframe...")
  val df = (1 to 1000000).map(_ => Random.nextInt(100).toString).toDF("value")//.repartition(10)
//    .repartition(4).cache()
//
//  val df = spark.read.option("header", "true").csv("/Users/patrick.cording/data/10K_Lending_Club_Loans.csv")
//    .select($"emp_title" as "value")
//    .filter($"value" isNotNull)
  println(df.count())

  val methods = List(
    new MapPartitionsMethod,
    new RddApiMethod,
    new AggregatorMethod,
    new AggregatorUdafMethod,
    new CustomUdafMethod
  )

  val timer = config(Key.exec.benchRuns -> 1, Key.verbose -> false)
//    .withWarmer { new Warmer.Default }
//    .withMeasurer { new Measurer.IgnoringGC }

  val results = methods.map(method => method.run(df))
  val timings = methods.map(method => timer.measure(method.run(df)).value)

  println(results)
  println(timings)

}