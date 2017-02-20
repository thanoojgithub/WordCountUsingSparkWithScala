package com

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.log4j.LogManager
import org.apache.log4j.Level

class WC {

  val logger = LoggerFactory.getLogger(classOf[WC])

  def getWC(): Unit = {
    logger.info("WC ***************************STARTED*******************************")
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile("D:/thanooj/work/testIn.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("D:/thanooj/work/testOut")
    logger.info("WC ***************************ENDED*******************************")
  }
}

object RunWC {

  //LogManager.getRootLogger().setLevel(Level.DEBUG);

  def main(args: Array[String]): Unit = {
    new WC().getWC()
  }
}