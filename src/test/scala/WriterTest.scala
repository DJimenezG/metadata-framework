
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import main.metadata.{Dataflow, Metadata, Output, OutputConfig}
import main.engine.Writer
import org.apache.commons.io.FileUtils

import java.io.File

class WriterTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("WriterTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("Escribe correctamente en modo overwrite") {
    val df = Seq(("Alice", 30), ("Bob", 25)).toDF("nombre", "edad")

    val outputPath = new File("src/test/resources/test_output")
    if (outputPath.exists()) FileUtils.deleteDirectory(outputPath)

    val outputCfg = OutputConfig(
      path = Some(outputPath.getAbsolutePath),
      format = Some("parquet"),
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

    val writtenDF = spark.read.parquet("src/test/resources/test_output")
    assert(writtenDF.columns.contains("nombre"))
  }
}
