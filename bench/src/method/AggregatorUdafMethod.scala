package method

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.apache.spark.sql.functions._


object MyAverageAgg2 extends Aggregator[Long, Average, Double] {
  def zero: Average = Average(0L, 0L)

  def reduce(buffer: Average, salary: Long): Average = {
    buffer.sum += salary
    buffer.count += 1
    buffer
  }

  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  def bufferEncoder: Encoder[Average] = Encoders.product[Average]

  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class AggregatorUdafMethod extends BaseMethod[Double] {
  override def run(df: DataFrame): Double = {
    val newUdaf = udaf(MyAverageAgg2)
    val result = df.select(newUdaf(col("salary")))
    result.first.getDouble(0)
  }
}
