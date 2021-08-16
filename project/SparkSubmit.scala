import sbtsparksubmit.SparkSubmitPlugin.autoImport._

object SparkSubmit {
  lazy val settings =
    SparkSubmitSetting("app",
      Seq("--class", "tpc.spark.fletcher.app.App")
    )
}
