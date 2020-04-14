package method

import buffer.LongestRunBuffer
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import run.Run

class LongestRunAggregator extends Aggregator[String, LongestRunBuffer, Option[Run]] {
  override def zero: LongestRunBuffer = new LongestRunBuffer

  override def reduce(b: LongestRunBuffer, a: String): LongestRunBuffer = b.put(a)

  override def merge(b1: LongestRunBuffer, b2: LongestRunBuffer): LongestRunBuffer = b1.merge(b2)

  override def finish(reduction: LongestRunBuffer): Option[Run] = reduction.eval

  override def bufferEncoder: Encoder[LongestRunBuffer] = Encoders.kryo[LongestRunBuffer]

  override def outputEncoder: Encoder[Option[Run]] = Encoders.kryo[Option[Run]]
}

class AggregatorMethod extends BaseMethod[Option[Run]] {
  override def run(df: DataFrame): Option[Run] = {
    val dataset = df.as(Encoders.STRING)
    val longestRunAggregator = (new LongestRunAggregator).toColumn
//    val udaf_ = udaf(new LongestRunAggregator, Encoders.kryo[LongestRunBuffer])
    dataset.select(longestRunAggregator).first
  }
}
