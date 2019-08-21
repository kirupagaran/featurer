package com.firemind.operation

import java.io.FileWriter
import java.sql.Timestamp
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}
import javax.xml.bind.DatatypeConverter

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import scopt.OptionParser
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{max, mean, min, stddev, to_date, to_utc_timestamp}
import org.apache.spark.sql.Column

import scala.collection.immutable.HashSet
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Failure, Success, Try}

/**
  * Created by Kirupa Devarajan
  * Creates a single dataframe and uses statistics output to filter the sparse features in the dataframe
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
object Chord1 {

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

    val data = spark.read.option("delimiter", "|").option("inferSchema", false).csv(factsPath).cache()
    val dictionaryDf = spark.read.option("delimiter", "|").csv(dictionaryPath).cache()
    val factsSchema = new StructType()
      .add("entity", "String")
      .add("attribute", "String")
      .add("value", "String")
      .add("time", "String")

    val factsRdd = data.rdd.cache()
    data.unpersist()
    val factsTempDf: DataFrame = spark.createDataFrame(factsRdd, factsSchema).cache()
    factsRdd.unpersist()
    import spark.implicits._
    val factsDf = factsTempDf.withColumn("datetime", ($"time".cast("timestamp"))).drop($"time")
      .na.fill(nullReplacement)
      .repartition($"entity")
      .cache()
    factsTempDf.unpersist()
    val monthsArray: Array[Int] = months.split(",").map(x => x.toInt)

    //createChord(spark, dictionary, data)
    val chordOutput = generateCatgChord(spark, factsDf, dictionaryDf, monthsArray, featureType).cache()
    chordOutput.write.csv(outputPath)
    chordOutput.unpersist()
    spark.stop()
  }


  def formDate(startDate: Timestamp, monthValue: Int): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val datetime = startDate.toString.split(" ")
    val calendar = DatatypeConverter.parseDateTime(datetime(0))
    calendar.add(Calendar.MONTH, monthValue)
    val maxDate = simpleDateFormat.format(calendar.getTime) + " " + datetime(1)
    maxDate
  }

  def getTimestamp(s: String): Timestamp = s match {
    case "" => new Timestamp(new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss").parse("0000-00-00 00:00:00.0").getTime)
    case _ => {
      val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
      Try(new Timestamp(format.parse(s).getTime)) match {
        case Success(t) => t
        case Failure(_) => new Timestamp(format.parse("0000-00-00 00:00:00.0").getTime)
      }
    }
  }


  def generateNumFlipsAndCounts(spark: SparkSession, factDf: DataFrame, months: Array[Int], entity: String, attributes: List[String], datetime: Timestamp): List[(String, String)] = {

    val minDate: Timestamp = datetime
    val factsDf: DataFrame = factDf.cache()
    var numflipsAndCountArray = ListBuffer[(String, String)]()
    import spark.implicits._
    val maxDate: String = formDate(minDate, months.max)
    val fullNumflips = factsDf.select($"value",$"datetime").filter((($"datetime" >= minDate) && ($"datetime" <= maxDate)) && ($"entity" === entity)).coalesce(1).cache()
    for (attribute <- attributes) {
      for (month <- months) {
        val tempMaxDate: String = formDate(minDate, month)
        //val np = fullNumflips.select($"value")
        val numflips:DataFrame = fullNumflips.select($"value").filter(($"attribute" === attribute) && ($"datetime" <= tempMaxDate)).coalesce(1).cache()
        //Add numflips
        numflipsAndCountArray += ((attribute + "_numflips_" + month + "_mth", numflips.count().toString()))
        //Add count
        numflipsAndCountArray += ((attribute + "_count_" + month + "_mth", numflips.distinct().count().toString()))
        numflips.unpersist()
      }
      fullNumflips.unpersist()
    }

    numflipsAndCountArray.toList
  }

  def generateGradientMaxMeanMinSd(spark: SparkSession, factDf: DataFrame, months: Array[Int], entity: String, attributes: List[String], datetime: Timestamp): List[(String, String)] = {

    val minDate: Timestamp = datetime
    val factsDf: DataFrame = factDf.cache()
    var gradientMaxMeanMinSd = ListBuffer[(String, String)]()
    import spark.implicits._
    for (attribute <- attributes) {
      for (month <- months) {
        val maxDate: String = formDate(minDate, month)
        val valueDf: DataFrame = factsDf.select($"value").filter((($"datetime" >= minDate) && ($"datetime" <= maxDate)) && ($"entity" === entity) && ($"attribute" === attribute)).filter(x => (!x.toString().contains("NA"))).map(x => x.getString(0).toInt).toDF("value")
        val lrData: Array[Double] = valueDf.collect().map(x => x.get(0).toString().toDouble)
        val calcLr:Double = LinearRegression(lrData).getSlope
        val lrVal:String = calcLr match{
          case x => {
            if (!x.isNaN) x.toString.substring(0,4)
            else "NA"
          }
        }
        //Add gradient
        gradientMaxMeanMinSd += ((attribute + "_gradient_" + month + "_mth",lrVal))
        //Add Max
        gradientMaxMeanMinSd += ((attribute + "_max_" + month + "_mth", valueDf.agg(max(valueDf.col("value"))).head.get(0).toString()))
        //Add Mean
        gradientMaxMeanMinSd += ((attribute + "_mean_" + month + "_mth", valueDf.agg(mean(valueDf.col("value"))).head.get(0).toString()))
        //Add Min
        gradientMaxMeanMinSd += ((attribute + "_min_" + month + "_mth", valueDf.agg(min(valueDf.col("value"))).head.get(0).toString()))
        //Add Standard Deviation
        gradientMaxMeanMinSd += ((attribute + "_sd_" + month + "_mth", valueDf.agg(stddev(valueDf.col("value"))).head.get(0).toString()))
      }
    }
    gradientMaxMeanMinSd.toList
  }

  def processFacts(spark: SparkSession, factsDf: DataFrame, features: Array[String], attributesAndValuesTuple: List[(String, String)], monthsArray: Array[Int], featureType: String): ArrayBuffer[String] = {
    //remove first element as it contains only entity and date
    val regularAttributes: List[(String, String)] = attributesAndValuesTuple.drop(1)
    val entity: String = attributesAndValuesTuple(0)._1
    val attributes: List[String] = attributesAndValuesTuple.drop(1).map(x => x._1.toString)
    val datetime: Timestamp = getTimestamp(attributesAndValuesTuple(0)._2)
    val months: Array[Int] = monthsArray
    val extraAttributes: List[(String, String)] = featureType match {
      case "categorical" => generateNumFlipsAndCounts(spark, factsDf, months, entity, attributes, datetime)
      case "continuous" => generateGradientMaxMeanMinSd(spark, factsDf, months, entity, attributes, datetime)
    }
    val allAttributesAndValues: List[(String, String)] = (regularAttributes ::: extraAttributes).sorted
    val allAttributes = allAttributesAndValues.map(attributesAndValues => attributesAndValues._1)
    var chordArray = ArrayBuffer[String]()
    //Merge entity (ID) and Date and add it to the chord
    chordArray += attributesAndValuesTuple(0)._1 + ":" + attributesAndValuesTuple(0)._2.split(" ")(0)
    for (feature <- features) {
      if (allAttributes.contains(feature)) {
        for (attributeAndValue <- allAttributesAndValues) {
          if (attributeAndValue._1 == feature)
            chordArray += attributeAndValue._2.toString
        }
      }
      else
        chordArray += "NA"
    }
    //Return chord array
    chordArray
  }


  def generateCatgChord(spark: SparkSession, factsDf: DataFrame, dictionaryDf: DataFrame, months: Array[Int], featureType: String): DataFrame = {

    import spark.implicits._
    //val features = dictionaryData.filter(x => x.toString.contains("tombstone")).filter(x => (x.toString().contains("type=categorical") || x.toString().contains("expression=num_flips") || x.toString().contains("expression=count")))
    //.map(x => x.getString(0).toString.split(":")(1)).collect().sorted
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

    val entityBasedData = factsDf.select($"entity", $"attribute", $"value", $"datetime").map(x => ((x(0).toString, x(3).toString), Array((x(1).toString, x(2).toString))))
      .cache()

    val entityKeyRdd = entityBasedData.rdd.reduceByKey(_ ++ _).map(x => x._1 +: x._2).flatMap(x => Array(x.toList)).cache()
    entityBasedData.unpersist()
    val entityKeyArray = entityKeyRdd.collect()
    entityKeyRdd.unpersist()
    val chordArray = entityKeyArray
      .map(attributesAndValuesTuple => processFacts(spark, factsDf, features, attributesAndValuesTuple, months, featureType))
    factsDf.unpersist()

    //Convert chord array to chord rdd
    val chordTempRdd: RDD[ArrayBuffer[String]] = spark.sparkContext.parallelize(chordArray).cache()
    val chordRdd: RDD[Row] = chordTempRdd.map(x => Row.fromSeq(x)).cache()
    chordTempRdd.unpersist()

    //Convert chord rdd to chord dataframe
    var chordDfSchema = Array[String]()
    chordDfSchema = "ID" +: features
    val fields = chordDfSchema.map(fieldName => StructField(fieldName, StringType))
    val schema = StructType(fields)
    val chordDf = spark.createDataFrame(chordRdd, schema)
    chordRdd.unpersist()
    chordDf
  }
}


