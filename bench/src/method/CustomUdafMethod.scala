package method

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import run.Run

class LongestRunUdaf extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Seq(
    StructField("value", StringType, false)
  ))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("length", LongType, false),
    StructField("char", StringType, false)
  ))

  override def dataType: DataType = StructType(Seq(
    StructField("length", LongType, false),
    StructField("char", StringType, false)
  ))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = "a"
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val bufferRun = Run(buffer.getString(1).head, buffer.getLong(0))
    val rowRun = Run.getLongestRun(input.getString(0))
    val longestRun = rowRun match {
      case Some(r) => Run.getLongestRun(bufferRun, r)
      case None => bufferRun
    }

    buffer.update(0, longestRun.length)
    buffer.update(1, longestRun.character)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buffer1Run = Run(buffer1.getString(1).head, buffer1.getLong(0))
    val buffer2Run = Run(buffer2.getString(1).head, buffer2.getLong(0))
    val longestRun = Run.getLongestRun(buffer1Run, buffer2Run)
    buffer1.update(0, longestRun.length)
    buffer1.update(1, longestRun.character)
  }

  override def evaluate(buffer: Row): Any = {
    val length = buffer.getLong(0)
    val char = buffer.getString(1).head
    (length, char)
  }
}

class CustomUdafMethod extends BaseMethod[Option[Run]] {
  override def run(df: DataFrame): Option[Run] = {
    val udaf = new LongestRunUdaf
    val result = df.select(udaf(col("value"))).collect()(0).getAs[Row](0)
//    df.createOrReplaceTempView("test")
//    df.sparkSession.udf.register("longest_run", new LongestRunUdaf)
//    val result = df.sparkSession.sql("SELECT longest_run(value) FROM test")
//      .collect()(0).getAs[Row](0)
    val (length, char) = (result.getLong(0), result.getString(1).head)
    if (length == 0) {
      None
    } else {
      Some(Run(char, length))
    }
  }
}
