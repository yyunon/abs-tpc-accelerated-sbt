name := "FPGA Pushdown Project"

ThisBuild / organization := "tpc.spark.fletcher"
ThisBuild / scalaVersion := "2.12.8"

mainClass in (Compile, run) := Some("tpc.spark.fletcher.app.App")

lazy val global = project
  .in(file("."))
  .settings(settings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    spark_extension,
    app
  )

lazy val spark_extension = project
  .settings(
    name := "spark_extension" settings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.arrow_memory
        .exclude("io.netty", "netty-common")
        .exclude("io.netty", "netty-buffer"),
      dependencies.arrow_vector,
      dependencies.guava,
      dependencies.protobuf
    )
  )
  .disablePlugins(AssemblyPlugin)

lazy val app = project
  .settings(
    name := "app" settings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.scopt
    )
  )
  .dependsOn(
    spark_extension
  )

lazy val dependencies =
  new {
    val scalaV = "2.12.8"
    val sparkV = "3.0.0"
    val arrowV = "0.17.0"
    val scoptV = "4.0.0"

    val spark_sql = "org.apache.spark" %% "spark-sql" % sparkV % "provided"
    val spark_catalyst = "org.apache.spark" %% "spark-catalyst" % sparkV % "provided"
    val spark_core = "org.apache.spark" %% "spark-core" % sparkV % "provided"
    val spark_launcher = "org.apache.spark" %% "spark-launcher" % sparkV % "provided"
    val scopt = "com.github.scopt" %% "scopt" % scoptV
    val scala_library = "org.scala-lang" % "scala-library" % scalaV
    val scala_reflect = "org.scala-lang" % "scala-reflect" % scalaV
    val scala_compiler = "org.scala-lang" % "scala-compiler" % scalaV
    val arrow_memory = "org.apache.arrow" % "arrow-memory" % arrowV
    val arrow_vector = "org.apache.arrow" % "arrow-vector" % arrowV
    val sl4j = "org.slf4j" % "slf4j-api" % "1.7.25"
    val guava = "com.google.guava" % "guava" % "23.0"
    val protobuf = "com.google.protobuf" % "protobuf-java" % "3.7.1"
  }

lazy val commonDependencies = Seq(
  dependencies.scala_library,
  dependencies.scala_reflect,
  dependencies.scala_compiler,
  dependencies.spark_launcher,
  dependencies.spark_catalyst,
  dependencies.spark_core,
  dependencies.spark_sql
)

// SETTINGS

lazy val settings =
  commonSettings ++
    wartremoverSettings ++
    scalafmtSettings

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val wartremoverSettings = Seq(
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(Wart.Throw)
)

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList("META-INF", "io.netty.versions.properties") =>
      MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)
