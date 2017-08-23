
package com.firemind.operation

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import javax.xml.bind.DatatypeConverter

import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by kirupa on 10/12/16.
  */
class ChordTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("jetty").setLevel(Level.OFF)
  Logger.getRootLogger.setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("sql_job")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/Dev/programs/others/ivory-spark/src/test/resources")
    .getOrCreate()

  import spark.implicits._

  "Categorical data " should " be loaded ivory-spark should create dynamic features" in {
    val featureType = "categorical"
    val data = spark.read.option("delimiter", "|").option("inferSchema", false).csv(getClass.getResource("/eavt_" + featureType + ".csv").getPath)
    val dictionaryDf = spark.read.option("delimiter", "|").csv(getClass.getResource("/dictionary_" + featureType + ".csv").getPath)
    val factsSchema = new StructType()
      .add("entity", "String")
      .add("attribute", "String")
      .add("value", "String")
      .add("time", "String")

    val dd = data.rdd

    val df: DataFrame = spark.createDataFrame(dd, factsSchema)
    import spark.implicits._
    val factsDf: DataFrame = df.withColumn("datetime", ($"time".cast("timestamp"))).drop($"time")
      .repartition($"datetime")
      .cache()
    val months: Array[Int] = "1,2,8".split(",").map(x => x.toInt)


    val outputDf = Chord.generateChord(spark,factsDf, months, months.length - 1)
    outputDf.foreach(println(_))
    outputDf.printSchema()
    //factsDf.withColumn("numflips_" + months(0), count($"value").over(Window.partitionBy("entity").orderBy($"datetime".cast("long")).rangeBetween(-0, months(0) * 30 * 86400))).printSchema()

    /*val calendar = DatatypeConverter.parseDateTime("2015-01-01 11:32:08")
    calendar.add(Calendar.MONTH, 7)

    val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
   println(format.parse("2015-01-01 11:32:08").getTime)
    println(format.parse("2015-06-01 11:32:08").getTime)

    println(format.parse("2015-03-02 11:32:08").getTime)
    println(format.parse("2015-09-02 11:32:08").getTime)*/


    //Chord.generateCatgChord(spark, factsDf, dictionaryDf, months, featureType).show
  }

  /*"Continuous data " should " be loaded ivory-spark should create dynamic features" in {
    val featureType = "continuous"
    val data = spark.read.option("delimiter", "|").option("inferSchema", false).csv(getClass.getResource("/eavt_" + featureType + ".csv").getPath)
    val dictionaryDf = spark.read.option("delimiter", "|").csv(getClass.getResource("/dictionary_" + featureType + ".csv").getPath)
    val factsSchema = new StructType()
      .add("entity", "String")
      .add("attribute", "String")
      .add("value", "String")
      .add("time", "String")

    val dd = data.rdd

    val df: DataFrame = spark.createDataFrame(dd, factsSchema)
    import spark.implicits._
    val factsDf = df.withColumn("datetime", ($"time".cast("timestamp"))).drop($"time")
      .repartition($"datetime")
      .cache()
    val months: Array[Int] = "1,2,8".split(",").map(x => x.toInt)
    Chord.generateCatgChord(spark, factsDf, dictionaryDf, months, featureType).show
  }

  "Linear Regression " should " successfully calculate slope" in {
    val dataArray: Array[Double] = Array(44, 22, 73, 14, 5)
    val lrObj = LinearRegression(dataArray)
    lrObj.getSlope.toString shouldEqual ("0.055757087196023104")
  }*/
}

