
/* SimpleApp.scala */
import org.apache.spark.mllib.fpm.FPClose
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

object TestApp {
  def main(args: Array[String]) {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    //conf.setAppName("FP Close")
    //conf.setMaster("yarn")
    conf.setAppName("Test")
    val sc = new SparkContext(conf)

    //val data = sc.textFile("/home/sahil/.spark_bin/spark/data/mllib/sample_fpgrowth.txt")
    //val data = sc.textFile("mushroom.dat")

    val data = sc.textFile("/home/sahil/p_project/datasets/uci_mushroom/agaricus-lepiota.data")

    val column_name_rdd =sc.textFile("/home/sahil/p_project/datasets/uci_mushroom/mushroom_cols.txt")

    val column_name = column_name_rdd.map(s => s.trim.split(',')).collect()


    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(','))
                                                .map(s => s.zip(column_name(0)))
                                                .map(s => s.map( t => t._2 + "." + t._1))

    val fpg = new FPClose()
      .setMinSupport(0.006)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

//    model.closedItemsets.collect().foreach { itemset =>
//      println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
//    }

    //model.closedItemsets.coalesce(1).saveAsTextFile("op_mushroom0.006")

    val closedItemsets = model.closedItemsets.map(c => (c.freq,c.items.toSet))

    val transactionsWithIndex = transactions.zipWithIndex.map{case (v,k) => (k+1,v)}
                                .map{case (k,v) => (k,v.toSet)}

    val joined = closedItemsets.cartesian(transactionsWithIndex)

    val closedItemsetsWithRows = joined
      .filter{x => x._1._2.subsetOf(x._2._2) && x._1._2.size >=3 }
      .map{x=> (x._1,x._2._1)}
        .groupByKey()

    closedItemsetsWithRows
      .map{x=> ("Count: " + x._1._1.toString(),x._1._2.mkString("{",",","}"),"Rows: " + x._2.toArray.mkString(","))}
      .coalesce(1).saveAsTextFile("op_mushroom0.006_withrow_grouped")

    //transactions.map(x=>x.mkString("{",",","}")).coalesce(1).saveAsTextFile("ip_mushroom_withColnames")

  }
}