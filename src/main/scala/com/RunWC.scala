package com

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class WC {

  val logger = LoggerFactory.getLogger(classOf[WC])

  def getWC(sc: SparkContext): Unit = {
    logger.info("WC ***************************STARTED*******************************")
    // Load our input data.
    //sc.textFile("hdfs:/work/input/TravelData.txt")
    val input = sc.textFile("./src/main/resources/testIn.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val countsAsc = words.map(word => (word, 1)).reduceByKey(_ + _).sortBy[Int](a => (a._2), true)
    countsAsc.foreach(println)
    println("--------------------")
    val countsDesc = words.map(word => (word, 1)).reduceByKey(_ + _).map(a => (a._2, a._1)).sortByKey()
    countsDesc.foreach(println)
    println("--------------------")
    val sortedKeyCollection = words.map(word => (word, 1)).reduceByKey(_ + _).map(e => e).sortByKey()
    sortedKeyCollection.foreach(println)
    println("--------------------")
    val maxOfCollection = words.map(word => (word, 1)).reduceByKey(_ + _).collectAsMap.maxBy(_._2)
    println("maxOfCollection :: " + maxOfCollection)

    // Save the word count back out to a text file, causing evaluation.
    // counts.saveAsTextFile("D:/thanooj/work/testOut")
    logger.info("WC ***************************ENDED*******************************")
  }

  def getTravelDetails(sc: SparkContext): Unit = {
    println("-----getTravelDetails-----")
    val input = sc.textFile("./src/main/resources/TravelData.txt")
    val splitRowOne = input.map(lines => lines.split('\t')).map(x => (x(2), 1)).reduceByKey(_ + _).sortBy[Int](a => (a._2), true)
    splitRowOne.take(3).foreach(println)
    println("-----")
    val splitRowTwo = input.map(lines => lines.split('\t')).map(x => (x(1), 1)).reduceByKey(_ + _).sortBy[Int](a => (a._2), true)
    splitRowTwo.take(3).foreach(println)
  }

}

object RunWC {

  //LogManager.getRootLogger().setLevel(Level.DEBUG);
  def main(args: Array[String]): Unit = {

    def getSparkContext(jobName: String): SparkContext = {
      val conf = new SparkConf().setAppName(jobName).setMaster("local") /*.set("spark.driver.allowMultipleContexts", "true")*/
      val sc = new SparkContext(conf)
      sc
    }
    val sc = getSparkContext("WC")
    val wc = new WC
    wc.getWC(sc)
    wc.getTravelDetails(sc)
  }
}


/* OUTPUT:
 *  
(this,2)
(is,2)
(a,2)
(test,3)
--------------------
(2,this)
(2,is)
(2,a)
(3,test)
--------------------
(a,2)
(is,2)
(test,3)
(this,2)
--------------------
maxOfCollection :: (test,3)
-----getTravelDetails-----
(COR,1)
(AKL,1)
(CGQ,1)
-----
(FLG,1)
(MGM,1)
(NUL,1)

 */