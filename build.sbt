name := "metadata-framework"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql"  % "3.4.1",
  // Delta Lake
  "io.delta" %% "delta-core" % "2.4.0",
  // JSON con Circe
  "io.circe" %% "circe-core"    % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "io.circe" %% "circe-parser"  % "0.14.6",
  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.11",
  "org.slf4j" % "log4j-over-slf4j" % "2.0.13",
  "org.slf4j" % "jul-to-slf4j" % "2.0.13",
  "org.slf4j" % "jcl-over-slf4j" % "2.0.13",
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp.filter { jar =>
    val name = jar.data.getName
    name.contains("spark") ||
      name.contains("log4j") ||       // 🔥 excluye log4j
      name.contains("slf4j-log4j") || // 🔥 excluye puentes erróneos
      name.contains("log4j-slf4j")    // 🔥 por si Spark trae este tipo
  }
}

// Opciones Java para pasar extensiones Delta (puede no ser suficiente por sí solo)
ThisBuild / javaOptions ++= Seq(
  "-Dspark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
  "-Dspark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

// Configuración del main class para que sbt lo encuentre
Compile / mainClass := Some("main.Main")

// Excluir Spark del assembly para evitar conflictos en tiempo de ejecución
import sbtassembly.AssemblyPlugin.autoImport._

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp.filter(_.data.getName.contains("spark"))
}