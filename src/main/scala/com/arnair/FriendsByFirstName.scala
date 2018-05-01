package com.arnair

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByFirstName {

  def parseLine(line: String): (String, Int) = {
    val list = line.split(",")
    (list(1), list(3).toInt)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("/home/arnair/Documents/udemy/SparkScala/fakefriends.csv")
    val parsedRDD = lines.map(parseLine)
    val reducedRDD = parsedRDD.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    val averageByName = reducedRDD.mapValues(x => x._1/x._2).collect()
    averageByName.sorted.foreach(println(_))
  }

}
