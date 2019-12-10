package com.firemindlabs.inputs

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.io._

object ConfigParser {
  def parse_json_config(filename: String): Map[String, Object] = {
    val json = Source.fromFile(filename)
    // parse
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = mapper.readValue[Map[String, Object]](json.reader())
    parsedJson
  }

}