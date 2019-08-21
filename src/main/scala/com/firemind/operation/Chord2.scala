package com.firemind.operation

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
import org.apache.spark.sql.expressions.Window

import scala.collection.immutable.HashSet
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Failure, Success, Try}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  *
  * Created by Kirupa Devarajan
  *
  * Sample Execution Command
  *
  * spark-submit \
  * --class Chord \
  * --master yarn \
  * --driver-memory 8g \
  * --executor-cores 4 \
  * --executor-memory 23g \
  * ivory-spark-assembly-1.0-SNAPSHOT.jar \
  * --project-path "s3://path" \
  * --feature-type "categorical" \
  * --null-replacement "NA" \
  * --facts-path "s3://path" \
  * --dictionary-path "s3://path" \
  * --output-path "/tmp/ivory-spark" \
  * --months "1,2"
  */
object Chord2 {

  type T = String
  type T1 = String

  case class Config(projectPath: String = "",
                    featureType: String = "",
                    nullReplacement: String = "",
                    factsPath: String = "",
                    dictionaryPath: String = "",
                    outputPath: String = "",
                    months: String = ""
                   )

  var warnings: Array[String] = Array()

  def main(args: Array[String]) {
    var projectPath: String = ""
    var featureType: String = ""
    var nullReplacement: String = ""
    var factsPath: String = ""
    var dictionaryPath: String = ""
    var outputPath: String = ""
    var months: String = ""

    //Parse cli options for params
    val parser = new OptionParser[Config]("DataFrameFilter") {
      head("DataFrame Filter")
      opt[String]('a', "project-path") required() action {
        (x, c) => c.copy(projectPath = x)
      } text "The path to the root directory of the project"
      opt[String]('f', "feature-type") required() action {
        (x, c) => c.copy(featureType = x)
      } text "Type of the feature. Possible values: categorical or continuous"
      opt[String]('s', "null-replacement") required() action { (x, c) =>
        c.copy(nullReplacement = x)
      } text "value to be replaced for null values in facts dataset"
      opt[String]('i', "facts-path") required() action { (x, c) =>
        c.copy(factsPath = x)
      } text "Location of the facts dataset"
      opt[String]('k', "dictionary-path") required() action { (x, c) =>
        c.copy(dictionaryPath = x)
      } text "Location of the dictionary file"
      opt[String]('o', "output-path") required() action { (x, c) =>
        c.copy(outputPath = x)
      } text "Location of Chord output"
      opt[String]('m', "months") required() action { (x, c) =>
        c.copy(months = x)
      } text "Comma seperated string of month intervals"
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        featureType = config.featureType
        nullReplacement = config.nullReplacement
        factsPath = config.factsPath
        dictionaryPath = config.dictionaryPath
        outputPath = config.outputPath
        months = config.months
        projectPath = config.projectPath
      case None =>
        // Error between seat and keyboard.
        println(s"Error with CLI options.")
        sys.exit(1)
    }
    // Args look good, run the program
    val spark: SparkSession = SparkSession.builder().appName("Ivory-Spark")
      .getOrCreate()
    import spark.implicits._
    val df1 = spark.read.option("header", "true").option("delimiter", "|").csv("")
    //.withColumn("time",unix_timestamp($"time", "yyyy-MM-dd"))
    val df2 = spark.read.option("header", "true").option("delimiter", "|").csv("")
      .withColumn("time", unix_timestamp($"time", "yyyy-MM-dd"))
    val dictionaryDf = spark.read.option("delimiter", "|").csv(dictionaryPath)
    val getTime = udf { x: String => x.split(":")(1) }
    val getEntity = udf { x: String => x.split(":")(0) }
    val labelsDf = df1.withColumn("rawtime", getTime($"id")).withColumn("entity", getEntity($"id")).withColumn("time", unix_timestamp($"rawtime", "yyyy-MM-dd")).drop("rawtime")
    val totalMonths: Array[Int] = "1,2,8".split(",").map(x => x.toInt)


    /*val outputDf = Chord.generateChord(spark,factsDf, months, months.length - 1)
    outputDf.foreach(println(_))
    outputDf.printSchema()*/

    val features: Array[String] = featureType match {
      case "categorical" => {
        dictionaryDf.filter(x => (x.toString().contains("type=categorical") || x.toString().contains("expression=num_flips") || x.toString().contains("expression=count")))
          .map(x => x.getString(0).toString.split(":")(1)).collect().sorted
      }
      case "continuous" => {
        dictionaryDf.filter(x => (x.toString().contains("type=continuous") || (!x.toString().contains("type=categorical") && !x.toString().contains("expression=num_flips") && !x.toString().contains("expression=count"))))
          .map(x => x.getString(0).toString.split(":")(1)).collect().sorted
      }
    }
    val features1 = Array("balance", "age")
    gen(spark, labelsDf, df2, features1, totalMonths, totalMonths.length - 1)
    spark.stop()
  }

  def gen(spark: SparkSession, labeldf: DataFrame, df22: DataFrame, features: Array[String], months: Array[Int], monthscnt: Int): DataFrame = {
    if (monthscnt >= 0) {
      val dtt: DataFrame = monthscnt match {
        case x if (monthscnt >= 0) => {
          val ddd = gen(spark, inter(spark, labeldf, df22, features, months, monthscnt), df22, features, months, monthscnt - 1)
          ddd
        }
      }
      dtt
    }
    else
      labeldf
  }


  def inter(spark: SparkSession, labelsDf: DataFrame, df2: DataFrame, features: Array[String], months: Array[Int], x: Int): DataFrame = {
    attr(spark, labelsDf, df2, features, features.length - 1, months, x)
  }

  def attr(spark: SparkSession, labeldf: DataFrame, df22: DataFrame, features: Array[String], featuresCnt: Int, months: Array[Int], monthscnt: Int): DataFrame = {
    if (featuresCnt >= 0) {
      val dtt: DataFrame = featuresCnt match {
        case x if (featuresCnt >= 0) => {
          val ddd = attr(spark, interFeat(spark, labeldf, df22, features, featuresCnt, months, monthscnt), df22, features, featuresCnt - 1, months, monthscnt)
          ddd
        }
      }
      dtt
    }
    else
      labeldf
  }

  def interFeat(spark: SparkSession, labelsDf: DataFrame, df2: DataFrame, features: Array[String], featureCnt: Int, months: Array[Int], x: Int): DataFrame = {

    val tempDf = labelsDf
    import spark.implicits._
    val lr = new LrUdaf()

    val month:Int = months(x)

    val dd:DataFrame = month match{
      case month if month == months.min => {

        labelsDf.join(
          df2, (((df2("time") > (labelsDf("time") - (month * 30 * 86400)))
            && (df2("time") < labelsDf("time")))
            && (df2("attribute") === features(featureCnt))
            && (labelsDf("entity") === df2("entity"))), "left"
        )
          .groupBy(labelsDf("time"),labelsDf("entity"))
          .agg(
            sum($"value").as(features(featureCnt) + "_sum_" + month),
            first($"value").as(features(featureCnt)),
            max($"value").as(features(featureCnt) + "_max_" + month),
            min($"value").as(features(featureCnt) + "_min_" +month),
            mean($"value").as(features(featureCnt) + "_mean_" + month),
            stddev($"value").as(features(featureCnt) + "_sd_" + month),
            lr($"value").as(features(featureCnt) + "_lr_" + month),
            sum($"value").as(features(featureCnt) + "_gradient_" + month),
            approxCountDistinct($"value", 0.01).as(features(featureCnt) + "_count_" + month))
          .orderBy(labelsDf("time"),labelsDf("entity"))
      }
      case _ =>{
        labelsDf.join(
          df2, (((df2("time") > (labelsDf("time") - (month * 30 * 86400)))
            && (df2("time") < labelsDf("time")))
            && (df2("attribute") === features(featureCnt))
            && (labelsDf("entity") === df2("entity"))), "left"
        )
          .groupBy(labelsDf("time"),labelsDf("entity"))
          .agg(
            sum($"value").as(features(featureCnt) + "_sum_" + month),
            max($"value").as(features(featureCnt) + "_max_" + month),
            min($"value").as(features(featureCnt) + "_min_" + month),
            mean($"value").as(features(featureCnt) + "_mean_" + month),
            stddev($"value").as(features(featureCnt) + "_sd_" + month),
            sum($"value").as(features(featureCnt) + "_sum_" + month),
            approxCountDistinct($"value", 0.01).as(features(featureCnt) + "_count_" + month))
          .orderBy(labelsDf("time"),labelsDf("entity"))
      }
    }

    /*val dd = labelsDf.join(
      df2, (((df2("time") > (labelsDf("time") - (months(x) * 30 * 86400)))
        && (df2("time") < labelsDf("time")))
        && (df2("attribute") === features(featureCnt))
        && (labelsDf("entity") === df2("entity"))), "left"
    )
      .groupBy(labelsDf("time"),labelsDf("entity"))
      .agg(
        sum($"value").as(features(featureCnt) + "_sum_" + months(x)),
        first($"value").as(features(featureCnt) + "_last_" + months(x)),
        max($"value").as(features(featureCnt) + "_max_" + months(x)),
        min($"value").as(features(featureCnt) + "_min_" + months(x)),
        mean($"value").as(features(featureCnt) + "_mean_" + months(x)),
        stddev($"value").as(features(featureCnt) + "_sd_" + months(x)),
        sum($"value").as(features(featureCnt) + "_sum_" + months(x)),
        approxCountDistinct($"value", 0.01).as(features(featureCnt) + "_count_" + months(x)))
      .orderBy(labelsDf("time"),labelsDf("entity"))*/

    tempDf.join(dd, Seq("time","entity"),"inner")

  }
}



