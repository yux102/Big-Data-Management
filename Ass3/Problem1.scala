package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Problem1 {
  def main(args: Array[String]) {

    val inputFile = args(0)
    val outputFolder = args(1)
    val topK = args(2)

    val conf = new SparkConf().setAppName("problem").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile)

    val lines = textFile.flatMap(x => x.split("\n"))
    val terms = lines.flatMap(x => x.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+")
      .filter(x => x.length > 0)
      .map(x => x.toLowerCase)
      .filter(x => x.charAt(0) >= 'a' && x.charAt(0) <= 'z')
      .distinct)

    implicit val sortByValueThenByWord = new Ordering[(String, Int)] {
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        if (x._2 == y._2) {
          x._1.compare(y._1)
        }
        else {
          y._2 - x._2
        }
      }
    }

    val termCountPairs = terms.map(x => (x, 1))
      .reduceByKey((x, y) => x + y).collect()
      .sortBy(x => x)
      .splitAt(topK.toInt)

    val results = sc.parallelize(termCountPairs._1.map(x => x._1 + '\t' + x._2))

    //    results.foreach(println)
    results.saveAsTextFile(outputFolder)
  }
}