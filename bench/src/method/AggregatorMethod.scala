package method

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}

case class Employee(salary: Long)
case class Average(var sum: Long, var count: Long)

// Taken from:
// https://spark.apache.org/docs/latest/sql-getting-started.html#type-safe-user-defined-aggregate-functions
object MyAverageAgg extends Aggregator[Employee, Average, Double] {
  def zero: Average = Average(0L, 0L)

  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  def bufferEncoder: Encoder[Average] = Encoders.product

  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class AggregatorMethod extends BaseMethod[Double] {
  override def run(df: DataFrame): Double = {
    // Technically, the select is redundant here as the Employee case class has
    // arity 1
    val dataset = df.select("salary").as(Encoders.product[Employee])
    val result = dataset.select(MyAverageAgg.toColumn)
    result.first
  }
}
