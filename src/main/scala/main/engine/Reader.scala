package main.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import main.metadata.Input
import org.slf4j.{Logger, LoggerFactory}

object Reader {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Carga todos los inputs definidos en el metadata y los devuelve en un Map[String, DataFrame]
   * @param spark  sesión de Spark
   * @param inputs Lista de objetos Input desde metadata con ruta, formato y opciones de lectura
   * @param year Año a sustituir dinámicamente en las rutas de entrada
   * @return Mapa de nombre lógico del input → DataFrame cargado
   */
  def loadInputs(spark: SparkSession, inputs: List[Input], year: String): Map[String, DataFrame] = {

    logger.info(s"- Carga los inputs del metadata para el año $year.......................................................")
    inputs.map { input =>
      val rawPath = input.config.path
      val format = input.config.format

      // Aseguramos rutas siempre válidas en el contenedor docker
      val resolvedPath =
        if (rawPath.startsWith("/")) {
          rawPath.replace("{{ year }}", year)
        } else {
          s"/app/" + rawPath.replace("{{ year }}", year)
        }

      val reader = spark.read
        .format(format)
        .options(Map(
          "header" -> input.spark_options.header,
          "delimiter" -> input.spark_options.delimiter
        ))

      val df = reader.load(resolvedPath)
      input.name -> df
    }.toMap
  }
}
