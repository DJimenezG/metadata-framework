package main

import org.apache.spark.sql.SparkSession
import metadata.{Dataflow, MetadataLoader}
import engine.{Reader, Transformer, Writer}
import org.slf4j.{Logger, LoggerFactory}

object Main {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Función principal que lee un archivo `metadata.json` que describe un flujo de datos (dataflow)
   * compuesto por entradas (inputs), transformaciones y salidas (outputs).
   * @param args Argumentos del script de entrada
   *  - args(0): Ruta al archivo `metadata.json` con la configuración del dataflow.
   *  - args(1): Año de referencia (formato yyyy) que se inyectará dinámicamente en las rutas de entrada/salida.
   *
   * Ejemplo de ejecución: spark-submit ... /app/metadata.json 2024
   */
  def main(args: Array[String]): Unit = {

    // Argumentos
    if (args.length != 2 || !args(1).matches("\\d{4}")) {
      throw new IllegalArgumentException(" Se debe pasar el año como argumento. Uso esperado: spark-submit ... <metadata.json> <año>. Ejemplo: /app/metadata.json 2024")
    }
    val metadataPath = args(0)
    val year = args(1)

    val spark = SparkSession.builder()
      .appName("metadata-framework")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Lectura y parseo del fichero metadata.json
    val metadata = MetadataLoader.load(metadataPath)

    metadata.dataflows.foreach { flow =>
      logger.info(s"- Ejecutando dataflow: ${flow.name} para año ${year}....................................................................")

      val inputs = Reader.loadInputs(spark, flow.inputs, year)
      val transformed = Transformer.applyTransformations(spark, flow.transformations, inputs)
      Writer.writeOutputs(spark, flow.outputs, transformed)
    }

    spark.stop()
  }
}
