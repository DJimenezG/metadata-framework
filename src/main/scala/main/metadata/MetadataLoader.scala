package main.metadata

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import java.nio.file.{Files, Paths}

object MetadataLoader {

  def load(path: String): Metadata = {
    val json = new String(Files.readAllBytes(Paths.get(path)), "UTF-8")
    parseMetadata(json)
  }

  def parseMetadata(jsonStr: String): Metadata = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue[Metadata](jsonStr)
  }
}