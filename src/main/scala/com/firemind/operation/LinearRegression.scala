package com.firemind.operation

import org.apache.commons.math3.stat.regression.SimpleRegression

/**
  * Created by kirupa on 31/07/2017.
  * Returns Linear Regression Object which has slope, intercept and regression related data
  */
class LinearRegression {
  def getLinearRegressionObject(data: Array[Double]): SimpleRegression = {
    val lr = new SimpleRegression(true)
    val indexedData = data.sorted.zipWithIndex
    val data2: Array[Array[(Double)]] = indexedData.map(x => Array(x._1, x._2))
    lr.addData(data2)
    lr
  }
}

object LinearRegression {
  def apply(data: Array[Double]): SimpleRegression = {
    val lrObj = new LinearRegression
    lrObj.getLinearRegressionObject((data))
  }
}
