package main.engine

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import io.delta.tables._
import main.metadata.{Output, OutputConfig}
import org.slf4j.{Logger, LoggerFactory}

object Writer {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Itera sobre los outputs definidos en el metadata y escribe los DataFrames correspondientes
   * según el tipo y el modo especificado.
   * @param spark SparkSession activa.
   * @param outputs Lista de objetos Output (con config y tipo) definidos en el metadata.
   * @param datasets Mapa con todos los DataFrames generados por las transformaciones previas.
   */
  def writeOutputs(spark: SparkSession, outputs: List[Output], datasets: Map[String, DataFrame]): Unit = {

    outputs.foreach { output =>
      val df = datasets(output.input)
      val cfg = output.config

      output.`type` match {
        case "file" =>
          writeFile(df, cfg)
        case "delta" =>
          cfg.save_mode match {
            case "append" =>
              writeDeltaAppend(df, cfg)
            case "merge" =>
              writeDeltaMerge(spark, df, cfg)
            case _ =>
              logger.info(s"- Modo de guardado no soportado para delta: ${cfg.save_mode}.......................................................")
          }
        case other =>
          logger.info(s"- Tipo de output no soportado: $other.......................................................")
      }
    }
  }

  /**
   * Valida y convierte el save_mode del config a un SaveMode válido de Spark
   * @param mode Modo de guardado leído del metadata.json.
   * @return SaveMode compatible con Spark.
   */
  private def parseSaveMode(mode: String): SaveMode = {

    mode.toUpperCase match {
      case "APPEND" | "OVERWRITE" | "IGNORE" | "ERRORIFEXISTS" =>
        SaveMode.valueOf(mode.charAt(0).toUpper + mode.substring(1).toLowerCase)
      case other =>
        throw new IllegalArgumentException(s" SaveMode no soportado: $other")
    }
  }

  /**
   * Escribe un DataFrame en un archivo de salida (parquet, json, csv...) según la configuración del metadata
   * @param df DataFrame a escribir.
   * @param cfg Configuración de salida (formato, path, modo, partición).
   */
  def writeFile(df: DataFrame, cfg: OutputConfig): Unit = {

    logger.info(s"- .....................................................................................................")
    logger.info(s"- writeFile.Escribiendo datos (${cfg.save_mode}).......................................................")
    logger.info(s"- .....................................................................................................")

    // Crear el directorio de salida si no existe
    new java.io.File(cfg.path.get).mkdirs()

    val writer = df.write
      .format(cfg.format.getOrElse("json"))
      .mode(parseSaveMode(cfg.save_mode))

    val finalWriter = cfg.partition match {
      case Some(partitionCol) => writer.partitionBy(partitionCol)
      case None               => writer
    }
    finalWriter.save(cfg.path.get)
    logger.info(s"- Escrito archivo en ${cfg.path.get} con modo ${cfg.save_mode}.......................................................")
  }

  /**
   * Escribe en una tabla Delta existente haciendo un 'Append'.
   * @param df DataFrame a añadir.
   * @param cfg Configuración con el nombre lógico de tabla y demás opciones.
   */
  private def writeDeltaAppend(df: DataFrame, cfg: OutputConfig): Unit = {

    logger.info(s"- ......................................................................................................................")
    logger.info(s"- writeFile.Escribiendo datos en formato Delta (${cfg.save_mode}).......................................................")
    logger.info(s"- ......................................................................................................................")

    val tableName = cfg.table.get

    // Mapeo interno de nombre lógico a ruta física
    val path = tableName match {
      case "raw_opendata_demo" =>
        "/app/data/output/opendata_demo/merge/raw_opendata_demo"
      case other =>
        throw new IllegalArgumentException(s"Tabla '$other' no está mapeada a ninguna ruta Delta válida.")
    }
    df.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)
    logger.info(s"- Append sobre tabla Delta: ${cfg.table.get}.......................................................")
  }
  /**
   * Realiza una operación Merge (Upsert) sobre una tabla Delta.
   * Si la tabla no existe, se crea automáticamente.
   * @param spark SparkSession activa.
   * @param df DataFrame con nuevos datos.
   * @param cfg Configuración del output, incluyendo claves primarias y path.
   */
  private def writeDeltaMerge(spark: SparkSession, df: DataFrame, cfg: OutputConfig): Unit = {

    logger.info(s"- ......................................................................................................................")
    logger.info(s"- writeFile.Escribiendo datos en formato Merge (${cfg.save_mode}).......................................................")
    logger.info(s"- ......................................................................................................................")

    val tableName = cfg.table.get
    val primaryKeys = cfg.primary_key.getOrElse {
      throw new IllegalArgumentException(s" Merge requiere definir primary_key para $tableName")
    }

    // Verificar si la tabla Delta existe
    val tableExists = spark.catalog.tableExists(tableName)

    val tablePath = cfg.path.getOrElse {
      throw new IllegalArgumentException(s" El campo path es obligatorio para merge en $tableName")
    }

    if (!tableExists) {
      logger.info(s"- La tabla $tableName no existe. Creando como Delta table con path: $tablePath.......................................................")
      df.write
        .format("delta")
        .mode("overwrite") // o "errorIfExists" si prefieres fallo explícito
        .save(tablePath)

      spark.sql(s"CREATE TABLE $tableName USING DELTA LOCATION '$tablePath'")
      logger.info(s"- Tabla $tableName creada correctamente como Delta table en $tablePath.......................................................")
    }

    // Eliminación de duplicados en el DataFrame fuente usando las claves primarias
    val dfDeduplicated = df.dropDuplicates(primaryKeys)

    // Realizar el merge
    val targetTable = DeltaTable.forPath(spark, tablePath)

    val mergeCondition = primaryKeys
      .map(pk => s"target.$pk = source.$pk")
      .mkString(" AND ")

    targetTable.as("target")
      .merge(dfDeduplicated.as("source"), mergeCondition)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"- Merge ejecutado sobre tabla Delta: $tableName.......................................................")
  }
}
