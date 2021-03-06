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
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source


/**
  *
  * Created by Kirupa Devarajan
  *
  * Sample Execution Command
  *
  * spark-submit \
  * --class com.firemindlabs.operation.FeatureGenerator \
  * --master local \
  * file:///tmp/examples/Featurer-assembly-1.0-SNAPSHOT.jar \
  * --static-features "status:int,balance:int" \
  * --force-categorical "null" \
  * --dynamic-features "sum,min,max,stddev" \
  * --labels-path "/tmp/examples/label_test.csv" \
  * --eavt-path "/tmp/examples/eavt_test.csv" \
  * --window "1,2," \
  * --null-replacement "null" \
  * --output-path "/tmp/featurer-output"
  */
object FeatureGenerator {

  def main(args: Array[String]) {

    start(args)

  }

  def start(args: Array[String]): Unit = {

    val input: ConstructInputs = ConstructInputs(args)

    println("\nRaw parameters received...")
    println("............................................\n")
    args.foreach(println(_))
    println("\n.........\n")
    println("\nAfter Parsing parameters succesfully...")
    println("............................................\n")
    input.getClass.getDeclaredFields.foreach(x => {
      x.setAccessible(true)
      println(x.getName() + " => " + x.get(input))
    })
    println("\n.........\n")


    val staticFeatures: Map[String, String] = input.staticFeatures.toString.split(",").map(x => x.toString.split(":")(1).toLowerCase match {
      case "string" => {
        (x.toString.split(":")(0), "categorical")
      }
      case _ => {
        ((x.toString.split(":")(0), "continuous"))
      }
    }).toMap




    val spark: SparkSession = SparkSession.builder().appName("Feature-Generator")
      .getOrCreate()
    import spark.implicits._

    val labelsDf = spark.read.option("header", "false").option("delimiter", "|").csv(input.labelsPath)

    val eavtDf = spark.read.option("header", "false").option("delimiter", "|").csv(input.eavtPath)


    val timeWindow = input.window.split(",").map(month => month.toInt)

    val processedData = preprocess(spark, eavtDf, labelsDf)

    val outputDf = generate(spark, processedData(0), processedData(1), staticFeatures, input.dynamicFeatures.split(","), timeWindow, timeWindow.length - 1)
      .drop("time", "_c0", "_c1")
    println("OUTPUT SAMPLE")
    println("+++++++++++++++")
    outputDf.show()
    outputDf.write.mode("overwrite").parquet(input.outputPath)
    spark.stop()
  }

  /*
  TODO: SCAN THE DATSET AND ENSURE THE DATATYPES OF THE COLUMNS ARE ONE OF THE FOLLOWING
        BOOLEAN, STRING for categorical
        BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, CHAT for continuous
        Use printschema() method
   */

  //TODO: CHECK NO HEADER FOR EAVT,

  /*
  PRE-PROCESSING
   */
  def preprocess(spark: SparkSession, dataDf: DataFrame, labelsDf: DataFrame): List[DataFrame] = {
    //spark implicits are required for all dollar usage in ($"column_name")
    import spark.implicits._

    //Data (convert date and time to timestamp)
    val data = dataDf.withColumn("_c3", unix_timestamp($"_c3", "yyyy-MM-dd HH:mm:ss"))
    val factsSchema = new StructType()
      .add("entity", "String")
      .add("attribute", "String")
      .add("value", "String")
      .add("time", "Long")
    val dataRdd = data.rdd
    val dataWithTimestamp: DataFrame = spark.createDataFrame(dataRdd, factsSchema)

    println("\nSample EAVT with timestamp in seconds...")
    println("............................................\n")
    dataWithTimestamp.show()

    val labels = labelsDf
      .withColumn("rawtime", $"_c1")
      .withColumn("entity", $"_c0")
      .withColumn("time", unix_timestamp($"rawtime", "yyyy-MM-dd HH:mm:ss"))

    List(labels, dataWithTimestamp)
  }


  /*
  GENERATE FEATURES - PRELIMINERY METHOD
   */
  def generate(spark: SparkSession, labeldf: DataFrame, eavtDf: DataFrame, staticFeatures: Map[String, String], dynamicFeatures: Array[String], months: Array[Int], monthscnt: Int): DataFrame = {

    if (monthscnt >= 0) {
      val dtt: DataFrame = monthscnt match {
        case x if (monthscnt >= 0) => {
          val ddd = generate(spark, level_1_recursion(spark, labeldf, eavtDf, staticFeatures, dynamicFeatures, months, monthscnt), eavtDf, staticFeatures, dynamicFeatures, months, monthscnt - 1)
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
  def level_1_recursion(spark: SparkSession, labelsDf: DataFrame, df2: DataFrame, staticFeatures: Map[String, String], dynamicFeatures: Array[String], months: Array[Int], x: Int): DataFrame = {
    level_2_recursion(spark, labelsDf, df2, staticFeatures, staticFeatures.size - 1, dynamicFeatures, months, x)
  }


  /*
   LEVEL-2-RECURSION
  */
  def level_2_recursion(spark: SparkSession, labeldf: DataFrame, df22: DataFrame, staticFeatures: Map[String, String], featuresCnt: Int, dynamicFeatures: Array[String], months: Array[Int], monthscnt: Int): DataFrame = {

    if (featuresCnt >= 0) {
      val dtt: DataFrame = featuresCnt match {
        case x if (featuresCnt >= 0) => {
          val ddd = level_2_recursion(spark, level_3_recursion(spark, labeldf, df22, staticFeatures, featuresCnt, dynamicFeatures, months, monthscnt), df22, staticFeatures, featuresCnt - 1, dynamicFeatures, months, monthscnt)
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
  def level_3_recursion(spark: SparkSession, labelsDf: DataFrame, df2: DataFrame, staticFeatures: Map[String, String], featureCnt: Int, dynamicFeatures: Array[String], months: Array[Int], x: Int): DataFrame = {
    val aggObj = new Aggregation()
    val tempDf = labelsDf
    import spark.implicits._
    val lr = new LrUdaf()
    val statFeature: Array[String] = staticFeatures.keys.toArray
    val month: Int = months(x)

    val getSubtractedTime = udf {
      x: String => {
        (DateTime.parse(x, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).minusMonths(month))
          .toString()
          .replace("T", " ")
          .split("\\.")(0)
      }
    }

    val labDf = labelsDf
      .withColumn("subtracted", getSubtractedTime($"rawtime").cast("string"))
      .withColumn("subtracted_time", unix_timestamp($"subtracted", "yyyy-MM-dd HH:mm:ss"))
      .drop("subtracted")

    val joinDf: DataFrame = labDf.join(
      df2, ((df2("time") > (labDf("subtracted_time")))
        && (df2("time") < labDf("time")))
        && (df2("attribute") === statFeature(featureCnt))
        && (labDf("entity") === df2("entity")), "left"
    ).drop("subtracted_time")

    val dd: DataFrame = aggObj.aggregated_columns(spark, labelsDf, joinDf, staticFeatures, featureCnt, dynamicFeatures, month)
    tempDf.join(dd, Seq("time", "entity"), "inner")

  }

}



