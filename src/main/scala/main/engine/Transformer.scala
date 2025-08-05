package main.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import main.metadata.{Transformation, TransformationConfig}
import org.slf4j.{Logger, LoggerFactory}

object Transformer {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Aplica transformaciones dinámicas a los DataFrames usando la lista de Transformations
   * @param spark sesión de Spark
   * @param transformations lista de transformaciones desde metadata
   * @param inputs Map de DataFrames con nombres definidos en los inputs
   * @return Map actualizado con cada transformación nombrada
   */
  def applyTransformations(spark: SparkSession, transformations: List[Transformation], inputs: Map[String, DataFrame]): Map[String, DataFrame] = {

    // Mapa mutable que irá guardando las salidas intermedias
    var datasets = inputs

    transformations.foreach { tf =>
      val inputDF = datasets(tf.input)

      tf.`type` match {
        case "add_fields" =>
          val fields = tf.config.fields.getOrElse(List())
          val enrichedDF = fields.foldLeft(inputDF) { (df, field) =>
            df.withColumn(field.name, expr(field.expression))
          }
          datasets += (tf.name -> enrichedDF)

        case "filter" =>
          val filterExpr = tf.config.filter.getOrElse("true")
          val filteredDF = inputDF.filter(expr(filterExpr))
          datasets += (tf.name -> filteredDF)

        case other =>
          logger.info(s"- Transformación no soportada: $other.......................................................")
      }
    }
    datasets
  }
}
