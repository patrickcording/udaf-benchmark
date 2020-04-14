package method

import org.apache.spark.sql.functions.{col, udaf}
import org.apache.spark.sql.{DataFrame, Encoders}
import run.Run


class AggregatorUdafMethod extends BaseMethod[Option[Run]] {
  override def run(df: DataFrame): Option[Run] = {
    // Testing UDAF wrapper new in Spark 3.0
    val dataset = df.as(Encoders.STRING)
    val longestRunAggregator = udaf(new LongestRunAggregator, Encoders.STRING)
    val res = dataset.select(longestRunAggregator(col("value")))

    // Slow:
    res.as(Encoders.kryo[Option[Run]]).first

    // Fast, but returns the serialized version of Option[Run]
//    res.first.getAs[Array[Byte]](0)
  }
}
