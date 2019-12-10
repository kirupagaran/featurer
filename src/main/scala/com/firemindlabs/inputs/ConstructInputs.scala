package com.firemindlabs.inputs
import com.firemindlabs.operation.FeatureGenerator

import scopt.OptionParser

/**
  * Created by kirupa on 10/12/19.
  */
class ConstructInputs {

  var staticFeatures: String = ""
  var forceCategorical: String = ""
  var dynamicFeatures: String = ""
  var labelsPath: String = ""
  var eavtPath: String = ""
  var nullReplacement: String = ""
  var outputPath: String = ""
  var window: String = ""
  var configPath: String = ""



  /*def validate(configPath:String,cmdLineArgs:Option[FeatureGenerator.Config]): Unit ={

    println("\nFollowing are the parameters configured...\n")
    println("............................................\n")
    if (configPath != "") {
      ConfigParser.parse_json_config(configPath)
        .foreach(configSet => println(configSet._1 + "=>" + configSet._2))
    }
    else{
      cmdLineArgs match {
        case Some(config) => {
          if (config.labelsPath == "" || config.eavtPath == ""){

          }
        }
      }
    }
    println("\n.........\n")
  }*/
}

object ConstructInputs{
  def apply(args:Array[String]): ConstructInputs={
    type T = String
    type T1 = String

    case class Config(
                       staticFeatures: String = "",
                       forceCategorical: String = "",
                       dynamicFeatures: String = "",
                       labelsPath: String = "",
                       eavtPath: String = "",
                       window: String = "",
                       nullReplacement: String = "",
                       outputPath: String = "",
                       configPath: String = ""
                     )

    var warnings: Array[String] = Array()


    var staticFeatures: String = ""
    var forceCategorical: String = ""
    var dynamicFeatures: String = ""
    var labelsPath: String = ""
    var eavtPath: String = ""
    var nullReplacement: String = ""
    var outputPath: String = ""
    var window: String = ""
    var configPath: String = ""

    //Parse cli options for params
    val parser = new OptionParser[Config]("Feature Generator") {
      head("Feature Generator")

      opt[String]('s', "static-features") action {
        (x, c) => c.copy(staticFeatures = x)
      } text "Comma seperate list of static features with their respective datatypes. Eg. \"age:int,gender:string\". Check documentation for the list of allowed datatypes"
      opt[String]('f', "force-categorical") action {
        (x, c) => c.copy(forceCategorical = x)
      } text "Comma seperated List of continuous features to be considered as categorical features."
      opt[String]('d', "dynamic-features") action { (x, c) =>
        c.copy(dynamicFeatures = x)
      } text "Comma seperated list of dynamic features to be generated."
      opt[String]('l', "labels-path") action {
        (x, c) => c.copy(labelsPath = x)
      } text "Path to the labels file"
      opt[String]('e', "eavt-path") action {
        (x, c) => c.copy(eavtPath = x)
      } text "Path to the EAVT file"
      opt[String]('w', "window") action { (x, c) =>
        c.copy(window = x)
      } text "Comma seperated string of month intervals"
      opt[String]('n', "null-replacement") action { (x, c) =>
        c.copy(nullReplacement = x)
      } text "value to be replaced for null values in output dataset"
      opt[String]('o', "output-path") action { (x, c) =>
        c.copy(outputPath = x)
      } text "Location of feature output file"
      opt[String]('c', "config-path") action {
        (x, c) => c.copy(configPath = x)
      } text "Local Path to the config file."
    }
    val constructInputObj = new ConstructInputs

    parser.parse(args, Config()) match {
      case Some(config) =>
        if (config.configPath != "") {
          val configParams: Map[String, Object] = ConfigParser.parse_json_config(config.configPath)
          constructInputObj.staticFeatures = configParams("static-features").toString
          constructInputObj.forceCategorical = configParams("force-categorical").toString
          constructInputObj.dynamicFeatures = configParams("dynamic-features").toString
          constructInputObj.labelsPath = configParams("labels-path").toString
          constructInputObj.eavtPath = configParams("eavt-path").toString
          constructInputObj.nullReplacement = configParams("null-replacement").toString
          constructInputObj.outputPath = configParams("output-path").toString
          constructInputObj.window = configParams("window").toString
          constructInputObj.configPath = configParams("config-path").toString

        }
        if (config.staticFeatures != "")
          constructInputObj.staticFeatures = config.staticFeatures
        if (config.forceCategorical != "")
          constructInputObj.forceCategorical = config.forceCategorical
        if (config.dynamicFeatures != "")
          constructInputObj.dynamicFeatures = config.dynamicFeatures
        if (config.labelsPath != "")
          constructInputObj.labelsPath = config.labelsPath
        if (config.eavtPath != "")
          constructInputObj.eavtPath = config.eavtPath
        if (config.nullReplacement != "")
          constructInputObj.nullReplacement = config.nullReplacement
        if (config.outputPath != "")
          constructInputObj.outputPath = config.outputPath
        if (config.window != "")
          constructInputObj.window = config.window
        if (config.configPath != "")
          constructInputObj.configPath = config.configPath


      case None =>
        // Error between seat and keyboard.
        println(s"Error with CLI options.")
        sys.exit(1)
    }
    constructInputObj
  }

}