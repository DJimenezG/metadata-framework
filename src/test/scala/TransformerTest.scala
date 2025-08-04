package metadata

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import main.engine.Transformer
import main.metadata.{AddField, Transformation, TransformationConfig}

class TransformerTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("TransformerTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("Aplica correctamente un filtro y añade campos") {
    val df = Seq(
      ("Ana", 30),
      ("Luis", 20)
    ).toDF("nombre", "edad")

    val trans = List(
      Transformation(
        name = "filtroEdad",
        `type` = "filter",
        input = "input_df",
        config = TransformationConfig(fields = Some(List(AddField("pais", "'España'"))), filter = Some("edad > 25")
        )
      ),
      Transformation(
        name = "addPais",
        `type` = "add_fields",
        input = "input_df",
        config = TransformationConfig(fields = Some(List(AddField("pais", "'España'"))), filter = None
        )
      )
    )

    val inputs = Map("input_df" -> df)
    val transformed = Transformer.applyTransformations(spark, trans, inputs)

    assert(transformed("input_df").columns.contains("pais"))
    assert(transformed("input_df").count() == 1)
  }
}
