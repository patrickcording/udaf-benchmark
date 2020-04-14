package method

import org.apache.spark.sql.DataFrame

trait BaseMethod[T] {
  def run(df: DataFrame): T
}
