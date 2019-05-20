package comp9313.ass3


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2 {
  def main(args: Array[String]) {

    val inputFile = args(0)
    val outputFolder = args(1)

    val conf = new SparkConf().setAppName("problem").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile)

    val lines = textFile.flatMap(x => x.split("\n"))

    val pairs = lines.map(x => (x.split("\t")(0), x.split("\t")(1)))
      .map(x => (x._2.toInt, x._1))
      .reduceByKey((x, y) => x + '|' + y).collect()
      .sortBy((x => x))
      .map(x => (x._1, x._2.split("\\|")
        .map(x => x.toInt)
        .sortBy(x => x)
        .mkString(",")))

    val results = sc.parallelize(pairs.map(x => x._1 + "\t" + x._2))

    //    results.foreach(println)
    results.saveAsTextFile(outputFolder)
  }

}
