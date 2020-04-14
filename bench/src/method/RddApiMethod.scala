package method
import buffer.LongestRunBuffer
import org.apache.spark.sql.DataFrame
import run.Run

class RddApiMethod extends BaseMethod[Option[Run]] {
  override def run(df: DataFrame): Option[Run] = {
    df.rdd.aggregate(new LongestRunBuffer)(
      (buffer, row) => buffer.put(row.getString(0)),
      (buffer1, buffer2) => buffer1.merge(buffer2)
    ).eval
  }
}
