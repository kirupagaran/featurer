
package com.firemind.operation

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import javax.xml.bind.DatatypeConverter

import org.apache.spark.rdd.RDD
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

    val df1 = spark.read.option("header", "true").option("delimiter", "|").csv(getClass.getResource("/label.csv").getPath)
    //.withColumn("time",unix_timestamp($"time", "yyyy-MM-dd"))
    val df2 = spark.read.option("header", "true").option("delimiter", "|").csv(getClass.getResource("/eavt.csv").getPath)
      .withColumn("time", unix_timestamp($"time", "yyyy-MM-dd"))
    val getTime = udf { x: String => x.split(":")(1) }
    val getEntity = udf { x: String => x.split(":")(0) }
    val labelsDf = df1.withColumn("rawtime", getTime($"id")).withColumn("entity", getEntity($"id")).withColumn("time", unix_timestamp($"rawtime", "yyyy-MM-dd")).drop("rawtime")


    val dd = data.rdd

    val df: DataFrame = spark.createDataFrame(dd, factsSchema)

    val factsDf: DataFrame = df.withColumn("datetime", ($"time".cast("timestamp"))).drop($"time")
      .repartition($"datetime")
      .cache()
    val months: Array[Int] = "1,2,8".split(",").map(x => x.toInt)


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




    def generateChord(spark: SparkSession, factsDf: DataFrame, labelsDf: DataFrame, months: Array[Int], monthCount: Int): DataFrame = {
      //val udlrObj = new UdafLr()
      import spark.implicits._
      //val windowRange = Window.partitionBy("entity").orderBy($"datetime".cast("long")).rangeBetween(-0, months(monthCount) * 30 * 86400)

      if (monthCount >= 0) {
        val dt: DataFrame = monthCount match {
          case x if (monthCount >= 0) => {
            println(months(monthCount))

            generateChord(spark, factsDf, (labelsDf.join(
              factsDf, factsDf("time") < labelsDf("time"), "left"
            )
              .groupBy(labelsDf("time"))
              .count
              .orderBy(labelsDf("time")))
              , months, monthCount - 1)
          }
        }
        dt
      }
      else
        df
    }



    def gen(labeldf: DataFrame, df22: DataFrame, features: Array[String], months: Array[Int], monthscnt: Int): DataFrame = {
      if (monthscnt >= 0) {
        val dtt: DataFrame = monthscnt match {
          case x if (monthscnt >= 0) => {
            val ddd = gen(inter(labeldf, df22, features, months, monthscnt), df22, features, months, monthscnt - 1)
            ddd
          }
        }
        dtt
      }
      else
        labeldf
    }


    def inter(labelsDf: DataFrame, df2: DataFrame, features: Array[String], months: Array[Int], x: Int): DataFrame = {
      attr(labelsDf, df2, features, features.length - 1, months, x)
    }

    def attr(labeldf: DataFrame, df22: DataFrame, features: Array[String], featuresCnt: Int, months: Array[Int], monthscnt: Int): DataFrame = {
      if (featuresCnt >= 0) {
        val dtt: DataFrame = featuresCnt match {
          case x if (featuresCnt >= 0) => {
            val ddd = attr(interFeat(labeldf, df22, features, featuresCnt, months, monthscnt), df22, features, featuresCnt - 1, months, monthscnt)
            ddd
          }
        }
        dtt
      }
      else
        labeldf
    }

    def interFeat(labelsDf: DataFrame, df2: DataFrame, features: Array[String], featureCnt: Int, months: Array[Int], x: Int): DataFrame = {

      val tempDf = labelsDf

      val dd = labelsDf.join(
        df2, ((df2("time") > (labelsDf("time") - (months(x) * 30 * 86400)))
          && (df2("time") < labelsDf("time"))
          && (df2("attribute") === features(featureCnt))
          && (labelsDf("entity") === df2("entity"))), "left"
      )
        .groupBy(labelsDf("time"))
        .agg(max($"value").as(features(featureCnt) + "_max_" + months(x)),
          min($"value").as(features(featureCnt) + "_min_" + months(x)),
          mean($"value").as(features(featureCnt) + "_mean_" + months(x)),
          stddev($"value").as(features(featureCnt) + "_sd_" + months(x)),
          count($"value").as(features(featureCnt) + "_numflips_" + months(x)),
          approxCountDistinct($"value", 0.01).as(features(featureCnt) + "_count_" + months(x)))
        .orderBy(labelsDf("time"))

      tempDf.join(dd, "time")

    }

    val features1 = Array("balance", "age")
    Chord2.gen(spark,labelsDf, df2, features1,months,months.length - 1).show
    //generateChord(spark,df2,labelsDf, months, months.length-1).show


    /*val resDf = df1.join(labelsDf, (((df1("time")-(2*30*86400)) < labelsDf("time"))&&(labelsDf("time") < df1("time"))), "left")
      .groupBy(from_unixtime(df1("time")),labelsDf("entity"),labelsDf("attribute"))
        .agg(mean($"value"),max($"value"),min($"value"),stddev($"value").as("sd"+"_2"),count($"value"),approxCountDistinct($"value",0.01))*/

    /*val aggregatedRdd: RDD[Row] = resDf.rdd.groupBy(r => r.getAs[String]("attribute"))
      .map(row =>
      // Mapping the Grouped Values to a new Row Object
      Row(row._2)
    )
    aggregatedRdd.foreach(println(_))*/


    //Convert chord rdd to chord dataframe
    var chordDfSchema = Array[String]()
    chordDfSchema = "ID" +: features
    val fields = chordDfSchema.map(fieldName => StructField(fieldName, StringType))
    val schema = StructType(fields)
    //val chordDf = spark.createDataFrame(chordRdd, schema)


    //resDf.rdd.groupBy()
    //resDf.withColumn("collate",)

    /*.withColumn("mean",mean($"value"))
    .withColumn("min",min($"value"))
    .withColumn("max",max($"value"))
      .withColumn("sd",stddev($"value"))
      .withColumn("count",count($"value"))*/


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

