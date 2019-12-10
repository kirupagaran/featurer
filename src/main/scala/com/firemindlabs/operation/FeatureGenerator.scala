package com.firemindlabs.operation

import java.io.FileWriter
import java.sql.Timestamp
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}
import javax.xml.bind.DatatypeConverter

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scopt.OptionParser
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Column

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Failure, Success, Try}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.firemindlabs.api.aggregation.Aggregation
import com.firemindlabs.inputs.{ConfigParser, ConstructInputs}

import scala.io.Source


/**
  *
  * Created by Kirupa Devarajan
  *
  * Sample Execution Command
  *
  * spark-submit \
  * --class com.firemindlabs.operation.FeatureGenerator \
  * --master yarn \
  * --driver-memory 8g \
  * --executor-cores 4 \
  * --executor-memory 23g \
  * Featurer-assembly-1.0-SNAPSHOT.jar \
  * --config-file "s3://path" \
  * --static-features "categorical" \
  * --force-categorical "NA" \
  * --dynamic-features "s3://path" \
  * --labels-path "s3://path" \
  * --eavt-path "/tmp/ivory-spark" \
  * --window "1,2" \
  * --null-replacement "" \
  * --output-path ""
  */
object FeatureGenerator {

  def main(args: Array[String]) {

   start(args)

  }

  /*
  TODO: SCAN THE DATSET AND ENSURE THE DATATYPES OF THE COLUMNS ARE ONE OF THE FOLLOWING
        BOOLEAN, STRING for categorical
        BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, CHAT for continuous
        Use printschema() method
   */

  /*
  PRE-PROCESSING
   */
  def preprocess(spark: SparkSession, dataDf: DataFrame, labelsDf: DataFrame): List[DataFrame] = {
    //spark implicits are required for all dollar usage in ($"column_name")
    import spark.implicits._

    //Data (convert date and time to timestamp)
    val data = dataDf.withColumn("_c3", unix_timestamp($"_c3", "yyyy-MM-dd"))
    val factsSchema = new StructType()
      .add("entity", "String")
      .add("attribute", "String")
      .add("value", "String")
      .add("time", "Long")
    val dataRdd = data.rdd
    val dataWithTimestamp: DataFrame = spark.createDataFrame(dataRdd, factsSchema)
    //val dataWithTimestamp: DataFrame = df.withColumn("datetime", ($"time".cast("timestamp"))).drop($"time")

    //Labels (convert date and time to timestamp)
    val getTime = udf { x: String => x.split(":")(1) }
    val getEntity = udf { x: String => x.split(":")(0) }
    val labels = labelsDf.withColumn("rawtime", getTime($"id")).withColumn("entity", getEntity($"id")).withColumn("time", unix_timestamp($"rawtime", "yyyy-MM-dd")).drop("rawtime")
    List(labels, dataWithTimestamp)
  }

  def start(args:Array[String]): Unit ={

    val input:ConstructInputs = ConstructInputs(args)

    println("\nFollowing are the parameters configured...\n")
    println("............................................\n")
    args.foreach(println(_))
    //args.foreach(println(_))
    input.getClass.getDeclaredFields.foreach(x=>{
      x.setAccessible(true)
      println(x.getName() +" => "+ x.get(input))
    })
    println("\n.........\n")
    val spark: SparkSession = SparkSession.builder().appName("Feature-Generator")
      .getOrCreate()
    import spark.implicits._

    val labelsDf = spark.read.option("header", "true").option("delimiter", "|").csv(input.labelsPath)

    val eavtDf = spark.read.option("header", "true").option("delimiter", "|").csv(input.eavtPath)

    val features: Array[String] = ConfigParser.parse_json_config(input.configPath)("static-features")
      .toString
      .split(",")
      .map(featureSet => featureSet.split(":")(0))

    val timeWindow = input.window.split(",").map(month => month.toInt)

    preprocess(spark, eavtDf, labelsDf)
    generate(spark, labelsDf, eavtDf, features, timeWindow, timeWindow.length - 1)
    spark.stop()
  }

  /*
  GENERATE FEATURES - PRELIMINERY METHOD
   */
  def generate(spark: SparkSession, labeldf: DataFrame, eavtDf: DataFrame, features: Array[String], months: Array[Int], monthscnt: Int): DataFrame = {
    if (monthscnt >= 0) {
      val dtt: DataFrame = monthscnt match {
        case x if (monthscnt >= 0) => {
          val ddd = generate(spark, level_1_recursion(spark, labeldf, eavtDf, features, months, monthscnt), eavtDf, features, months, monthscnt - 1)
          ddd
        }
      }
      dtt
    }
    else
      labeldf
  }

  /*
   LEVEL-1-RECURSION
  */
  def level_1_recursion(spark: SparkSession, labelsDf: DataFrame, df2: DataFrame, features: Array[String], months: Array[Int], x: Int): DataFrame = {
    level_2_recursion(spark, labelsDf, df2, features, features.length - 1, months, x)
  }


  /*
   LEVEL-2-RECURSION
  */
  def level_2_recursion(spark: SparkSession, labeldf: DataFrame, df22: DataFrame, features: Array[String], featuresCnt: Int, months: Array[Int], monthscnt: Int): DataFrame = {

    if (featuresCnt >= 0) {
      val dtt: DataFrame = featuresCnt match {
        case x if (featuresCnt >= 0) => {
          val ddd = level_2_recursion(spark, level_3_recursion(spark, labeldf, df22, features, featuresCnt, months, monthscnt), df22, features, featuresCnt - 1, months, monthscnt)
          ddd
        }
      }
      dtt
    }
    else
      labeldf
  }

  /*
   LEVEL-3-RECURSION
  */
  def level_3_recursion(spark: SparkSession, labelsDf: DataFrame, df2: DataFrame, features: Array[String], featureCnt: Int, months: Array[Int], x: Int): DataFrame = {
    val aggObj = new Aggregation()
    val tempDf = labelsDf
    import spark.implicits._
    val lr = new LrUdaf()

    val month: Int = months(x)

    val joinDf: DataFrame = labelsDf.join(
      df2, ((df2("time") > (labelsDf("time") - (month * 30 * 86400)))
        && (df2("time") < labelsDf("time")))
        && (df2("attribute") === features(featureCnt))
        && (labelsDf("entity") === df2("entity")), "left"
    )

    val dd: DataFrame = aggObj.aggregated_columns(spark, labelsDf, joinDf, features, featureCnt, month)
    tempDf.join(dd, Seq("time", "entity"), "inner")

  }

}



