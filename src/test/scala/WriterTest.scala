package metadata

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.scalatest.funsuite.AnyFunSuite
import main.metadata.{Output, OutputConfig, Dataflow, Metadata}
import main.engine.Writer

class WriterTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("WriterTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("Escribe correctamente en modo overwrite") {
    val df = Seq(("Alice", 30), ("Bob", 25)).toDF("nombre", "edad")

    val outputCfg = OutputConfig(
      path = Some("src/test/resources/test_output"),
      format = Some("csv"),
      save_mode = "overwrite",
      partition = None,
      table = None,
      primary_key = None
    )
    val output = Output(
      name = "output1",
      `type` = "sink",
      input = "input1",
      config = outputCfg
    )

    Writer.writeFile(df, outputCfg)

    val writtenDF = spark.read.option("header", "true").csv("src/test/resources/test_output")
    assert(writtenDF.columns.contains("nombre"))
  }
}
