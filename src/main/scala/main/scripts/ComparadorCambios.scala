package main.scripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.slf4j.{Logger, LoggerFactory}

object ComparadorCambios {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Comparador de Cambios entre años")
      .master("local[*]")
      .getOrCreate()

    val df2024 = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("/app/data/input/opendata_demo/poblacion2024.csv")
      .withColumnRenamed("total", "total_2024")

    val df2025 = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("/app/data/input/opendata_demo/poblacion2025.csv")
      .withColumnRenamed("total", "total_2025")

    // Encontrar filas que cambian: inner join por claves + condiciones de diferencia
    val joinCols = Seq("provincia", "municipio", "sexo")

    //val joined = df2024.as("a").join(df2025.as("b"), joinCols, "inner")
    //val cambiadas = joined.filter("a.total != b.total")
    val joined = df2024.join(df2025, joinCols, "full_outer")

    val cambios = joined.filter(
      col("total_2024").isNull ||
        col("total_2025").isNull ||
        col("total_2024") =!= col("total_2025")
    )

    logger.info(s"- .........................................................................................")
    logger.info(s"- Filas cambiadas entre 2024 y 2025:.......................................................")
    cambios.show(false)
    logger.info(s"- .........................................................................................")
    //cambiadas.selectExpr("a.*").show(false)

    spark.stop()
  }
}
