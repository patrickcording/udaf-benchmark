package method
import org.apache.spark.sql.{DataFrame, Row}

class RddApiMethod extends BaseMethod[Double] {
  override def run(df: DataFrame): Double = {
    val finalBuffer = df.select("salary")
      .rdd.aggregate(Average(0, 0))(
      // Update function:
      (buffer: Average, row: Row) => {
        buffer.sum += row.getLong(0)
        buffer.count += 1
        buffer
      },
      // Merge function:
      (buffer1: Average, buffer2: Average) => {
        buffer1.sum += buffer2.sum
        buffer1.count += buffer2.count
        buffer1
      }
    )

    finalBuffer.sum.toDouble / finalBuffer.count
  }
}
