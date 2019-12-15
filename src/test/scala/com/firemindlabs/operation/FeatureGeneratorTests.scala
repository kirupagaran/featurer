
package com.firemindlabs.operation


import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import com.firemindlabs.inputs.ConfigParser
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

  "Preprocessing of data " should " be done successfully" in {
    val args: Array[String] = Array(
      "--config-path",
      getClass.getResource("/config.json").getPath,
      "--static-features",
      "status:int,balance:string",
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
      "/tmp"
    )
    FeatureGenerator.start(args)
    /*val data = spark.read.option("delimiter", "|").option("inferSchema", false).csv(getClass.getResource("/eavt_test.csv").getPath)
    val labels = spark.read.option("header", "false").option("delimiter", "|").csv(getClass.getResource("/label_test.csv").getPath)
    FeatureGenerator.preprocess(spark, data, labels)(0).printSchema()
    FeatureGenerator.preprocess(spark, data, labels)(1).printSchema()

    ConfigParser.parse_json_config(getClass.getResource("/config.json").getPath)
      .foreach(configSet => println(configSet._1 + " => " + configSet._2))*/
  }

 /* "Categorical data " should " be loaded ivory-spark should create dynamic staticFeatures" in {
    val featureType = "categorical"

    val dictionaryDf = spark.read.option("delimiter", "|").csv(getClass.getResource("/dictionary_" + featureType + ".csv").getPath)
    dictionaryDf.show()

    val months: Array[Int] = "1,2".split(",").map(x => x.toInt)
    val features1 = Map("age"->"continuous","balance"->"continuous" )
    val data = spark.read.option("delimiter", "|").option("inferSchema", false).csv(getClass.getResource("/eavt_test.csv").getPath)
    val labels = spark.read.option("header", "false").option("delimiter", "|").csv(getClass.getResource("/label_test.csv").getPath)


    FeatureGenerator.preprocess(spark,data,labels)
    FeatureGenerator.generate(
      spark,
      FeatureGenerator.preprocess(spark,data,labels)(0),
      FeatureGenerator.preprocess(spark,data,labels)(1),
      features1,
      months,
      months.length - 1
    ).show(false)
  }
*/
  /*val outputDf = Chord.generateChord(spark,factsDf, months, months.length - 1)
    outputDf.foreach(println(_))
    outputDf.printSchema()*/

  /*val staticFeatures: Array[String] = featureType match {
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



    def generate(labeldf: DataFrame, eavtDf: DataFrame, staticFeatures: Array[String], months: Array[Int], monthscnt: Int): DataFrame = {
      if (monthscnt >= 0) {
        val dtt: DataFrame = monthscnt match {
          case x if (monthscnt >= 0) => {
            val ddd = generate(level_1_recursion(labeldf, eavtDf, staticFeatures, months, monthscnt), eavtDf, staticFeatures, months, monthscnt - 1)
            ddd
          }
        }
        dtt
      }
      else
        labeldf
    }


    def level_1_recursion(labelsDf: DataFrame, df2: DataFrame, staticFeatures: Array[String], months: Array[Int], x: Int): DataFrame = {
      level_2_recursion(labelsDf, df2, staticFeatures, staticFeatures.length - 1, months, x)
    }

    def level_2_recursion(labeldf: DataFrame, eavtDf: DataFrame, staticFeatures: Array[String], featuresCnt: Int, months: Array[Int], monthscnt: Int): DataFrame = {
      if (featuresCnt >= 0) {
        val dtt: DataFrame = featuresCnt match {
          case x if (featuresCnt >= 0) => {
            val ddd = level_2_recursion(level_3_recursion(labeldf, eavtDf, staticFeatures, featuresCnt, months, monthscnt), eavtDf, staticFeatures, featuresCnt - 1, months, monthscnt)
            ddd
          }
        }
        dtt
      }
      else
        labeldf
    }

    def level_3_recursion(labelsDf: DataFrame, df2: DataFrame, staticFeatures: Array[String], featureCnt: Int, months: Array[Int], x: Int): DataFrame = {

      val tempDf = labelsDf

      val dd = labelsDf.join(
        df2, ((df2("time") > (labelsDf("time") - (months(x) * 30 * 86400)))
          && (df2("time") < labelsDf("time"))
          && (df2("attribute") === staticFeatures(featureCnt))
          && (labelsDf("entity") === df2("entity"))), "left"
      )
        .groupBy(labelsDf("time"))
        .agg(max($"value").as(staticFeatures(featureCnt) + "_max_" + months(x)),
          min($"value").as(staticFeatures(featureCnt) + "_min_" + months(x)),
          mean($"value").as(staticFeatures(featureCnt) + "_mean_" + months(x)),
          stddev($"value").as(staticFeatures(featureCnt) + "_sd_" + months(x)),
          count($"value").as(staticFeatures(featureCnt) + "_numflips_" + months(x)),
          approxCountDistinct($"value", 0.01).as(staticFeatures(featureCnt) + "_count_" + months(x)))
        .orderBy(labelsDf("time"))

      tempDf.join(dd, "time")

    }*/


  //generateChord(spark,df2,labelsDf, months, months.length-1).show


  /*val resDf = df1.join(labelsDf, (((df1("time")-(2*30*86400)) < labelsDf("time"))&&(labelsDf("time") < df1("time"))), "left")
      .groupBy(from_unixtime(df1("time")),labelsDf("entity"),labelsDf("attribute"))
        .agg(mean($"value"),max($"value"),min($"value"),stddev($"value").as("sd"+"_2"),count($"value"),approxCountDistinct($"value",0.01))*/

  /*val aggregatedRdd: RDD[Row] = resDf.rdd.groupBy(r => r.getAs[String]("attribute"))
      .map(row =>
      // Mapping the Grouped Values to a new Row Object
      Row(row._2)
    )
    aggregatedRdd.foreach(println(_))


    //Convert chord rdd to chord dataframe
    var chordDfSchema = Array[String]()
    chordDfSchema = "ID" +: staticFeatures
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

  /*"Continuous data " should " be loaded ivory-spark should create dynamic staticFeatures" in {
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
*/
}
