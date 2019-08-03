
/* SimpleApp.scala */
import org.apache.spark.mllib.fpm.FPClose
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession

object TestApp {
  def main(args: Array[String]) {

    if (args.length != 2) {
      //println("Usage filename.jar input_path output_path")
      System.err.println("Usage filename.jar input_csv_file_path output_path")
      System.exit(1)
    }


    val spark = SparkSession
      .builder
      //.master("local[*]")
      //.master("yarn")
      .appName("FP Close Sample")
      .getOrCreate()

    val sc = spark.sparkContext

    //conf.setMaster("yarn")
    //conf.setAppName("Test")


    //val data = sc.textFile("/home/sahil/.spark_bin/spark/data/mllib/sample_fpgrowth.txt")
    //val data = sc.textFile("mushroom.dat")

    //val data = sc.textFile("/home/sahil/p_project/datasets/uci_mushroom/agaricus-lepiota.data")
    //val data = sc.textFile("/home/sahil/p_project/datasets/uci_plants/plants.data")

    val data = sc.textFile(args(0))

    //val column_name_rdd =sc.textFile("/home/sahil/p_project/datasets/uci_mushroom/mushroom_cols.txt")

    //val column_name = column_name_rdd.map(s => s.trim.split(',')).collect()


    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(','))
                                               // .map(s => s.zip(column_name(0)))
                                                //.map(s => s.map( t => t._2 + "." + t._1))

    val fpg = new FPClose()
      //.setMinSupport(0.10)
      .setNumPartitions(10)
      //.setMinItems(3)
      .setGenRowNumbers(true)
    val model = fpg.run(transactions)

    model.closedItemsets.saveAsTextFile(args(1))

//    model.closedItemsets.collect().foreach { itemset =>
//      println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
//    }

    //model.closedItemsets.coalesce(1).saveAsTextFile("op_mushroom0.006")

//    val closedItemsets = model.closedItemsets.map(c => (c.freq,c.items.toSet))
//
//    val transactionsWithIndex = transactions.zipWithIndex.map{case (v,k) => (k+1,v)}
//                                .map{case (k,v) => (k,v.toSet)}
//
//    val joined = closedItemsets.cartesian(transactionsWithIndex)
//
//    val closedItemsetsWithRows = joined
//      .filter{x => x._1._2.subsetOf(x._2._2) && x._1._2.size >=3 }
//      .map{x=> (x._1,x._2._1)}
//        .groupByKey()



//    model.closedItemsets
//      .map{x=> ("Count: " + x.freq.toString(),x.items.mkString("{",",","}"),"Rows: " + x.rowNumbers.mkString(","))}
//      .coalesce(1).saveAsTextFile("op_plants_withrows0.10_new")

    //transactions.map(x=>x.mkString("{",",","}")).coalesce(1).saveAsTextFile("ip_mushroom_withColnames")

  }
}