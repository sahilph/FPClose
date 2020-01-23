import org.apache.spark.mllib.fpm.FPClose
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test_FPClose {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("FP Close Sample")
      .getOrCreate()

    val sc = spark.sparkContext

    val data = sc.textFile("/home/sahil/.spark_bin/spark/data/mllib/sample_fpgrowth.txt") //This is the default file located at $SPARK_HOME/data/mllib/sample_fpgrowth.txt

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPClose()
      .setMinSupport(0.3)
      .setMinItems(2) //Set min Number of Items to consider for closed Itemset (Default: 0, Meaning dont filter)
      .setGenRowNumbers(true) // Generate Row numbers where the closed Itemset was found (Default: false) (Extensive Operation. Use with caution !!)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    /*
    Prints in form of [closed_itemset],frequency,[row_numbers]
     */
    model.closedItemsets.collect().foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")}, ${itemset.freq}, ${itemset.rowNumbers.mkString("{", ",", "}")}")
    }
  }

}
