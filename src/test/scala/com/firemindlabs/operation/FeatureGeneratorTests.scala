
package com.firemindlabs.operation


import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import com.firemindlabs.inputs.{ConfigParser, ConstructInputs}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import javax.xml.bind.DatatypeConverter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by kirupa on 10/12/18.
  */
class FeatureGeneratorTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("jetty").setLevel(Level.OFF)
  Logger.getRootLogger.setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("sql_job")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///tmp/resources")
    .getOrCreate()
  //spark implicits are required for all dollar usage in ($"column_name")
  import spark.implicits._

  "Commandline arguments " should "override config file and parsed successfully" in {
    val args: Array[String] = Array(
      "--config-path",
      getClass.getResource("/config.json").getPath,
      "--static-features",
      "status:int,balance:int",
      "--force-categorical",
      "null",
      "--dynamic-features",
      "sum,min,max,stddev",
      "--labels-path",
      getClass.getResource("/label_test.csv").getPath,
      "--eavt-path",
      getClass.getResource("/eavt_test.csv").getPath,
      "--window",
      "1,2,6,18",
      "--null-replacement",
      "null",
      "--output-path",
      " /tmp/featurer-output"
    )
    val input: ConstructInputs = ConstructInputs(args)
    input.staticFeatures should be("status:int,balance:int")
    input.dynamicFeatures should be("sum,min,max,stddev")
  }


  "Config file" should " be parsed successfully" in {
    val args: Array[String] = Array(
      "--config-path",
      getClass.getResource("/config_test.json").getPath
    )
    val input: ConstructInputs = ConstructInputs(args)
    input.staticFeatures should be("balance:int")
    input.dynamicFeatures should be("stddev,max")
    input.eavtPath should be("firemindlabs/firemind/featurer/src/test/resources/eavt_test.csv")
    input.labelsPath should be("firemindlabs/firemind/featurer/src/test/resources/label_test.csv")
    input.forceCategorical should be("balance")
    input.nullReplacement should be("NA")
    input.window should be("1,4,6")
    input.outputPath should be("/tmp/featurer-output")

  }

  "Dynamic Features" should "be generated successfully" in {
    val args: Array[String] = Array(
      "--static-features",
      "status:int,balance:int",
      "--force-categorical",
      "null",
      "--dynamic-features",
      "sum,min,max,stddev",
      "--labels-path",
      getClass.getResource("/label_test.csv").getPath,
      "--eavt-path",
      getClass.getResource("/eavt_test.csv").getPath,
      "--window",
      "1,2,6,18",
      "--null-replacement",
      "null",
      "--output-path",
      "/tmp/featurer-output"
    )
    val input: ConstructInputs = ConstructInputs(args)
    input.staticFeatures should be("status:int,balance:int")
    input.dynamicFeatures should be("sum,min,max,stddev")

    FeatureGenerator.start(args)
  }
}
