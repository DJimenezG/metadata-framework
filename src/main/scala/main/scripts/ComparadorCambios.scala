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
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

    // Columnas a comparar
    val joinCols = Seq("provincia", "municipio", "sexo")

    val cambios = df2024
      .join(df2025, joinCols, "full_outer")
      .filter(col("total_2024").isNull ||
        col("total_2025").isNull ||
        col("total_2024") =!= col("total_2025"))

    logger.info(s"- .........................................................................................")
    logger.info(s"- Filas cambiadas entre 2024 y 2025:.......................................................")
    cambios.show(false)
    logger.info(s"- .........................................................................................")

    // Persistimos la tabla de cambios
    val tablePath = "/app/data/output/opendata_demo/comparador_cambios"
    // Crear el directorio de salida si no existe
    new java.io.File(tablePath).mkdirs()

    // Guardar cambios en formato Delta (o Parquet)
    cambios.write
      .format("delta")  // Puedes cambiar a "parquet" si prefieres
      .mode("overwrite")
      .save(tablePath)
    logger.info(s"- Cambios exportados a $tablePath en formato Delta")
    logger.info(s"- .........................................................................................")

    if (!spark.catalog.tableExists("comparador_cambios")) {
      spark.sql(s"CREATE TABLE comparador_cambios USING DELTA LOCATION '$tablePath'")
    }

    spark.stop()
  }
}
