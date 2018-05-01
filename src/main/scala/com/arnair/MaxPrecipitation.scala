package com.arnair

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MaxPrecipitation {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(0), fields(1), fields(2), fields(3).toDouble)
  }
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("/home/arnair/Documents/udemy/SparkScala/1800.csv")
    val parsedRDD = lines.map(parseLine)

    val filteredRDD = parsedRDD.filter(x => x._3 =="PRCP").map(x => (x._2, x._4))
    val result = filteredRDD.reduce((x,y) => if(x._2 > y._2) x else y)

  }

}
