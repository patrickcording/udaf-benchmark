package method

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

// Taken from
// https://spark.apache.org/docs/latest/sql-getting-started.html#untyped-user-defined-aggregate-functions
object MyAverageUdaf extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  def dataType: DataType = DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}

class CustomUdafMethod extends BaseMethod[Double] {
  override def run(df: DataFrame): Double = {
    val result = df.select(MyAverageUdaf(col("salary")))
    result.collect()(0).getDouble(0)
  }
}
