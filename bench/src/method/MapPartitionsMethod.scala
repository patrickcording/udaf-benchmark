package method
import buffer.LongestRunBuffer
import org.apache.spark.sql.{DataFrame, Encoders}
import run.Run


class MapPartitionsMethod extends BaseMethod[Option[Run]] {
  override def run(df: DataFrame): Option[Run] = {
    implicit val encoder = Encoders.kryo[LongestRunBuffer]

    // Map step
    val map = df.mapPartitions(partition => {
      val buffer = new LongestRunBuffer
      partition.foreach(row => {
        val rowVal = row.getString(0)
        buffer.put(rowVal)
      })
      Iterator.single(buffer)
    })

    // Reduce step
    val reduce = map.reduce {
      (buffer1, buffer2) => buffer1.merge(buffer2)
    }

    reduce.eval
  }
}
