package metadata

import scala.io.Source
import org.scalatest.funsuite.AnyFunSuite
import main.metadata.MetadataLoader

class MetadataLoaderTest extends AnyFunSuite {

  test("Carga y parseo correcto de metadata.json") {
    val jsonStr = Source.fromResource("test_metadata.json").mkString
    val metadata = MetadataLoader.parseMetadata(jsonStr)

    // Verificación de un solo dataflow cargado
    assert(metadata.dataflows.length == 1)

    // Comprobaciónb del parse correcto de output_path
    assert(metadata.dataflows.head.outputs.head.config.path.contains("/app/data/output/opendata_demo/last"))

    //Comprobacion de menos una transformación definida (dataflow completo)
    assert(metadata.dataflows.head.transformations.nonEmpty)

    // Verificar nombres de inputs/outputs
    val dataflows = metadata.dataflows.head
    assert(dataflows.inputs.head.name == "input_2024")
    assert(dataflows.outputs.head.name == "output_demo")

    //Verificar configuración de lectura/escritura
    val dataflowsInputs = metadata.dataflows.head.inputs.head
    assert(dataflowsInputs.config.format.contains("csv"))
    assert(dataflowsInputs.spark_options.header.contains("true"))

    // Verificar tipos de transformación
    val transformationTypes = metadata.dataflows.head.transformations.map(_.name)
    assert(transformationTypes.contains("add_date"))
    assert(transformationTypes.contains("filter_rows"))
  }
}
