package main.scripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object ValidadorCalidad {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    // Argumento
    if (args.length != 1 || !args(0).matches("\\d{4}")) {
      throw new IllegalArgumentException("Debes pasar un año como argumento. Ejemplo: spark-submit ... ValidadorCalidad.scala 2025")
    }
    val year = args(0)

    val spark = SparkSession.builder()
      .appName(s"Validador de Calidad para el año $year")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val inputPath = s"/app/data/input/opendata_demo/poblacion${year}.csv"
    logger.info(s"- Validando calidad de datos para el año $year desde: $inputPath.................................")

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputPath)
      .withColumn("total", col("total").cast("int"))

    // Pivotamos por 'sexo' para comparar
    val pivoted = df
      .groupBy("provincia", "municipio") // PK's del origen
      .pivot("sexo", Seq("Ambos sexos", "Hombres", "Mujeres"))
      .agg(first("total"))
      .withColumnRenamed("Ambos sexos", "Ambos_sexos")

    // Validamos que Ambos_sexos = Hombres + Mujeres
    val errores = pivoted.filter(col("Ambos_sexos") =!= col("Hombres") + col("Mujeres"))

    logger.info(s"- ...............................................................................................")
    logger.info(s"- Filas con problemas de calidad de datos para el año $year:..................................................")
    errores.show(false)
    logger.info(s"- Número total de filas con errores: ${errores.count()}.......................................................")
    logger.info(s"- ............................................................................................................")


    // Persistir errores como tabla Delta
    val outputPath = s"/app/data/output/opendata_demo/errores_calidad/$year"
    val tableName = s"errores_calidad_$year"

    errores.write
      .format("delta")
      .mode("overwrite")
      .save(outputPath)

    logger.info(s"- Errores guardados en formato Delta en: $outputPath")

    // Registrar la tabla en el catálogo si no existe
    if (!spark.catalog.tableExists(tableName)) {
      spark.sql(s"CREATE TABLE $tableName USING DELTA LOCATION '$outputPath'")
      logger.info(s"- Tabla registrada como '$tableName' en el catálogo de Spark.......................................")
    } else {
      logger.info(s"- La tabla '$tableName' ya estaba registrada.......................................................")
    }

    spark.stop()
  }
}
