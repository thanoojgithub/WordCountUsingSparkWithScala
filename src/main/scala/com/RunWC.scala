package com

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.ShuffleDependency

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
    countsAsc.toDebugString
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
  def doEvenOdd() = {
    val (even, odd) = (1 to 10).toList.partition { x => x % 2 == 0 }
    even.foreach { println }
    odd.foreach { println }
    val evenNum = (1 to 10).toList.filter { x => x % 2 == 0 }
    val oddNum = (1 to 10).toList.filter { x => (x % 2 != 0 || x == 1) }

  }

  def pairRDD(sc: SparkContext): Unit = {
    val pairedrdd = sc.parallelize(List((1, 2), (1, 2), (3, 4), (5, 6)))

    val pairedrdd1 = pairedrdd.reduceByKey((x, y) => x + y)
    val pairedrdd2 = pairedrdd1.mapValues { x => x + 1 }
    val pairedrdd3 = pairedrdd2.sortByKey()
    println("--------------------pairedrdd.toDebugString start------------------------------------")
    pairedrdd3.toDebugString
    println("--------------------pairedrdd.toDebugString end------------------------------------")
    pairedrdd3.foreach(println)

    //case ????
  }

  def doMoreParallelize(sc: SparkContext): Unit = {
    //10 number of partitions to cut the dataset into
    val rdd = sc.parallelize(1 to 10, 10).groupBy(_ % 2)
    val rddObj = rdd.saveAsObjectFile(_)
    println(rddObj)
    rdd.foreach(println)
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

  def doParallelize(sc: SparkContext): Unit = {
    val distData = sc.parallelize(Array(1, 2, 3, 4, 5))
    println("----doParallelize-----------")
    println(distData.toDebugString)
    println("----doParallelize-----------")
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
    wc.pairRDD(sc)
    wc.doParallelize(sc)
    wc.doEvenOdd()
    wc.doMoreParallelize(sc)
    println("---------sc.stop()----start----------")
    sc.stop()
    println("---------sc.stop()----end----------")
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
--------------------pairedrdd.toDebugString start------------------------------------
--------------------pairedrdd.toDebugString end------------------------------------
(1,5)
(3,5)
(5,7)
----doParallelize-----------
(1) ParallelCollectionRDD[36] at parallelize at RunWC.scala:82 []
----doParallelize-----------
2
4
6
8
10
1
3
5
7
9
(0,CompactBuffer(2, 4, 6, 8, 10))
(1,CompactBuffer(1, 3, 5, 7, 9))
* 
*/