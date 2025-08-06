
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
      (30),
      (20)
    ).toDF("edad")

    val trans = List(
      Transformation(
        name = "filtroEdad",
        `type` = "filter",
        input = "input_df",
        config = TransformationConfig(
          filter = Some("edad > 25"),
          fields = None
        )
      ),
      Transformation(
        name = "addPais",
        `type` = "add_fields",
        input = "filtroEdad",
        config = TransformationConfig(
          fields = Some(List(AddField("pais", "'España'"))),
          filter = None
        )
      )
    )

    val inputs = Map("input_df" -> df)
    val transformed = Transformer.applyTransformations(spark, trans, inputs)

    val result = transformed("addPais")
    result.show()

    assert(result.columns.contains("pais"))
    assert(result.count() == 1)
  }
}
