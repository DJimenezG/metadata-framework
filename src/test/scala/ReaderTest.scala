package metadata

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import main.metadata.{Input, InputConfig, SparkOptions, Dataflow, Metadata}
import main.engine.Reader

class ReaderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("ReaderTest")
    .master("local[*]")
    .getOrCreate()

  test("Lee correctamente un archivo CSV desde ruta relativa") {
    val path = getClass.getResource("/test_data.csv").getPath
    val sparkOpts = SparkOptions("true", ",")
    val input = Input("test_input", "csv", InputConfig(path, "csv"), sparkOpts)

    val dataflow = Dataflow("df1", List(input), Nil, Nil)
    val metadata = Metadata(List(dataflow))

    val df = Reader.loadInputs(spark, metadata.dataflows.head.inputs)("test_input")

    assert(df.count() > 0)
    assert(df.columns.contains("nombre")) // Ajusta a tu CSV real
  }
}
