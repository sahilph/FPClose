
/* SimpleApp.scala */
import org.apache.spark.mllib.fpm.FPClose
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

object TestApp {
  def main(args: Array[String]) {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("FP Close")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/sahil/.spark_bin/spark/data/mllib/sample_fpgrowth.txt")
    //val data = sc.textFile("/home/sahil/p_project/datasets/mushroom.dat")

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPClose()
      .setMinSupport(0.3)
      //.setNumPartitions(10)
    val model = fpg.run(transactions)

    model.closedItemsets.collect().foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
    }


  }
}