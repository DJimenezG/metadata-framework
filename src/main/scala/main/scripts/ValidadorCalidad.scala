package main.scripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object ValidadorCalidad {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Validador de Calidad")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("/app/data/input/opendata_demo/poblacion2025.csv")
      .withColumn("total", col("total").cast("int"))

    // Pivotamos por 'sexo' para comparar
    val pivoted = df
      .groupBy("provincia", "municipio") // Si hay más claves, añádelas aquí
      .pivot("sexo", Seq("Ambos sexos", "Hombres", "Mujeres"))
      .agg(first("total"))
      .withColumnRenamed("Ambos sexos", "Ambos_sexos")

    // Validamos que Ambos_sexos = Hombres + Mujeres
    val errores = pivoted.filter(col("Ambos_sexos") =!= col("Hombres") + col("Mujeres"))

    logger.info(s"- ...............................................................................................")
    logger.info(s"- Filas con problemas de calidad de datos:.......................................................")
    errores.show(false)
    logger.info(s"- Número total de filas con errores: ${errores.count()}.......................................................")
    logger.info(s"- ............................................................................................................")

    spark.stop()
  }
}
