package com.arnair

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AmountByCustomer {

  def parse(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalAmountByCustomer")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("/home/arnair/Documents/udemy/SparkScala/customer-orders.csv")
    val customerAmountRDD = lines.map(parse)
    val sumByCustomer = customerAmountRDD.reduceByKey((x, y) => x+y)

    val results = sumByCustomer.map(x => (x._2, x._1)).collect()
    results.sorted.foreach(println(_))
  }

}
