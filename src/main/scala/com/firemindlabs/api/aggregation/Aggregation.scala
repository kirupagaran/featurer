package com.firemindlabs.api.aggregation

import com.firemindlabs.operation.LrUdaf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession, _}

import scala.collection.mutable.ListBuffer

/**
  * Created by kirupa on 05/12/19.
  */
class Aggregation {
  def aggregated_columns(spark: SparkSession, labelsDf: DataFrame, data: DataFrame, features: Array[String], featureCnt: Int, month: Int): DataFrame = {
    val aggs = Array("skew")
    var aggFuncs:Seq[Column] = Seq(sum(data("value")))
    var aggsf = new ListBuffer[Column]()

    val allFeatures = Map("age"->"continuous","balance"->"continuous" )

    aggs.map {
      x => x match {
        // ONLY CONTINUOUS FEATURES
        case "min" => {
          if (allFeatures(features(featureCnt)) == "continuous")
            aggsf += min(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else if (allFeatures(features(featureCnt)) == "categorical"){
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }

        }
        case "max" => {
          if (allFeatures(features(featureCnt)) == "continuous")
            aggsf += max(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else if (allFeatures(features(featureCnt)) == "categorical"){
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }
        case "stddev" => {
          if (allFeatures(features(featureCnt)) == "continuous")
            aggsf += stddev(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else if (allFeatures(features(featureCnt)) == "categorical"){
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }
        case "sum" => {
          if (allFeatures(features(featureCnt)) == "continuous")
            aggsf += sum(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else if (allFeatures(features(featureCnt)) == "categorical"){
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }
        case "mean" => {
          if (allFeatures(features(featureCnt)) == "continuous")
            aggsf += mean(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else if (allFeatures(features(featureCnt)) == "categorical"){
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }
        case "skew" => {
          if (allFeatures(features(featureCnt)) == "continuous")
            aggsf += skewness(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else if (allFeatures(features(featureCnt)) == "categorical"){
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }
        // CONTINUOUS and CATEGORICAL FEATURES
        case "count" => {
          if (allFeatures(features(featureCnt)) == "continuous" || allFeatures(features(featureCnt)) == "categorical" )
            aggsf += count(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else {
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }
        case "first" => {
          if (allFeatures(features(featureCnt)) == "continuous" || allFeatures(features(featureCnt)) == "categorical" )
            aggsf += first(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else {
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }
        case "last" => {
          if (allFeatures(features(featureCnt)) == "continuous" || allFeatures(features(featureCnt)) == "categorical" )
            aggsf += last(data("value")).as(features(featureCnt)+"_"+x+"_" + month)
          else {
            aggsf += (lit("NULL").as(features(featureCnt) +"_"+x+"_" + month))
          }
        }

        /*case "slope" => {
          val lr = new LrUdaf()
          aggsf += lr.(data("value")).as(features(featureCnt) + "_first_" + month)
        }*/
        case _ => {
          aggsf.map(x => println(x))
        }
      }

    }


    val c:Seq[Column] = aggsf.to

    data.groupBy(labelsDf("time"), labelsDf("entity"))
      .agg(c.head, c.tail: _*)
      .orderBy(labelsDf("time"), labelsDf("entity"))
  }
}
