import coursier.maven.MavenRepository
import mill._
import mill.scalalib._


object bench extends ScalaModule {
  def scalaVersion = "2.12.10"

  override def repositories = super.repositories ++ Seq(
    MavenRepository("https://oss.sonatype.org/content/repositories/snapshots")
  )

  val localSparkJarPath = os.Path(sys.env("UDAF_SPARK_JAR_PATH"))
  def unmanagedClasspath = T {
    if (!ammonite.ops.exists(localSparkJarPath)) Agg()
    else Agg.from(ammonite.ops.ls(localSparkJarPath).map(PathRef(_)))
  }

  override def ivyDeps = Agg(
    ivy"com.storm-enroute::scalameter-core:0.19-SNAPSHOT"
  )
}
