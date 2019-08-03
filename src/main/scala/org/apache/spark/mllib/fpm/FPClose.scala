/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.fpm

import java.{util => ju}
import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.fpm.FPClose._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
  * Model trained by [[FPClose]], which holds closed frequent itemsets.
  * @param closedItemsets closed frequent itemset, which is an RDD of `ClosedItemset`
  * @tparam Item item type
  */
@Since("2.4.0")
class FPCloseModel[Item: ClassTag] @Since("2.4.0")(
                                                    @Since("2.4.0") val closedItemsets: RDD[ClosedItemset[Item]],
                                                    @Since("2.4.0") val itemSupport: Map[Item, Double])
  extends Saveable with Serializable {

  @Since("1.3.0")
  def this(closedItemsets: RDD[ClosedItemset[Item]]) = this(closedItemsets, Map.empty)



  /**
    * Save this model to the given path.
    * It only works for Item datatypes supported by DataFrames.
    *
    * This saves:
    *  - human-readable (JSON) model metadata to path/metadata/
    *  - Parquet formatted data to path/data/
    *
    * The model may be loaded using `FPGrowthModel.load`.
    *
    * @param sc  Spark context used to save model data.
    * @param path  Path specifying the directory in which to save this model.
    *              If the directory already exists, this method throws an exception.
    */
  @Since("2.0.0")
  override def save(sc: SparkContext, path: String): Unit = {
    FPCloseModel.SaveLoadV1_0.save(this, path)
  }

  override protected val formatVersion: String = "1.0"
}

@Since("2.4.0")
object FPCloseModel extends Loader[FPCloseModel[_]] {

  @Since("2.4.0")
  override def load(sc: SparkContext, path: String): FPCloseModel[_] = {
    FPCloseModel.SaveLoadV1_0.load(sc, path)
  }

  private[fpm] object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private val thisClassName = "org.apache.spark.mllib.fpm.FPCloseModel"

    def save(model: FPCloseModel[_], path: String): Unit = {
      val sc = model.closedItemsets.sparkContext
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Get the type of item class
      val sample = model.closedItemsets.first().items(0)
      val className = sample.getClass.getCanonicalName
      val classSymbol = runtimeMirror(getClass.getClassLoader).staticClass(className)
      val tpe = classSymbol.selfType

      val itemType = ScalaReflection.schemaFor(tpe).dataType
      val fields = Array(StructField("items", ArrayType(itemType)),
        StructField("freq", LongType), StructField("rowNumbers", ArrayType(LongType)))
      val schema = StructType(fields)
      val rowDataRDD = model.closedItemsets.map { x =>
        Row(x.items.toSeq, x.freq, x.rowNumbers.toSeq)
      }
      spark.createDataFrame(rowDataRDD, schema).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): FPCloseModel[_] = {
      implicit val formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val freqItemsets = spark.read.parquet(Loader.dataPath(path))
      val sample = freqItemsets.select("items").head().get(0)
      loadImpl(freqItemsets, sample)
    }

    def loadImpl[Item: ClassTag](freqItemsets: DataFrame, sample: Item): FPCloseModel[Item] = {
      val freqItemsetsRDD = freqItemsets.select("items", "freq", "rowNumbers").rdd.map { x =>
        val items = x.getAs[Seq[Item]](0).toArray
        val freq = x.getLong(1)
        val rowNumbers = x.getAs[Seq[Long]](2).toArray
        new ClosedItemset(items, freq, rowNumbers)
      }
      new FPCloseModel(freqItemsetsRDD)
    }
  }
}

/**
  * A parallel FP-Close algorithm to mine closed frequent itemsets. This algorithm is based on the idea
  * of the one described in <a href="http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.3.6241">
  *  which is in turn parallelized further using the the concepts described in the following paper
  * <a href="http://dx.doi.org/10.1145/1454008.1454027">Li et al., PFP: Parallel FP-Growth for Query
  * Recommendation</a>. PFP distributes computation in such a way that each worker executes an
  * independent group of mining tasks. The FP-Growth algorithm is described in
  * <a href="http://dx.doi.org/10.1145/335191.335372">Han et al., Mining frequent patterns without
  * candidate generation</a>.
  *
  * @param minSupport the minimal support level of the frequent pattern, any pattern that appears
  *                   more than (minSupport * size-of-the-dataset) times will be output
  * @param numPartitions number of partitions used by parallel FP-growth
  * @param minItems The minimum number of items in closed itemset.
  * @param genRows Generates the rownumbers in which the closed itemset as found.
  *
  * @see <a href="http://en.wikipedia.org/wiki/Association_rule_learning">
  * Association rule learning (Wikipedia)</a>
  *
  */
@Since("2.4.0")
class FPClose private[spark] (
                                private var minSupport: Double,
                                private var numPartitions: Int,
                                private var minItems: Long,
                                private var genRows: Boolean) extends Logging with Serializable {

  /**
    * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
    * as the input data and minimum items as 0 (meaning all items)}.
    *
    */
  @Since("1.3.0")
  def this() = this(0.3, -1, 0, false)

  /**
    * Sets the minimal support level (default: `0.3`).
    *
    */
  @Since("1.3.0")
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got ${minSupport}")
    this.minSupport = minSupport
    this
  }

  /**
    * Sets the number of partitions used by parallel FP-growth (default: same as input data).
    *
    */
  @Since("1.3.0")
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  /**
    * Sets the minimum Items to be considered (default: `3`).
    *
    */
  @Since("FP Close Mod")
  def setMinItems(minItems: Long ): this.type = {
    require(minItems >= 0 ,
      s"Minimum Items should be >=0 but got ${minItems}")
    this.minItems = minItems
    this
  }

  /**
    * Sets the whether to generate the row numbers or not (default: `false`).
    *
    */
  @Since("FP Close Mod")
  def setGenRowNumbers(genRows: Boolean ): this.type = {

    this.genRows = genRows
    this
  }

  /**
    * Computes an FP-Close model that contains closed frequent itemsets.
 *
    * @param data input data set, each element contains a transaction
    * @return an [[FPCloseModel]]
    *
    */
  @Since("2.4.0")
  def run[Item: ClassTag](data: RDD[Array[Item]]):
  FPCloseModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItemsCount = genFreqItems(data, minCount, partitioner)
    var closedItemsets = genClosedItemsets(data, minCount, freqItemsCount.map(_._1), partitioner)
    if(this.genRows){ closedItemsets = genRowNumbers(data,closedItemsets) }
    val itemSupport = freqItemsCount.map {
      case (item, cnt) => item -> cnt.toDouble / count
    }.toMap
    new FPCloseModel(closedItemsets, itemSupport)
  }



  /**
    * Java-friendly version of `run`.
    */
  @Since("1.3.0")
  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPCloseModel[Item] = {
    implicit val tag = fakeClassTag[Item]
    run(data.rdd.map(_.asScala.toArray))
  }

  /**
    * Generates frequent items by filtering the input data using minimal support level.
    * @param minCount minimum count for frequent itemsets
    * @param partitioner partitioner used to distribute items
    * @return array of frequent patterns and their frequencies ordered by their frequencies
    */
  private def genFreqItems[Item: ClassTag](
                                            data: RDD[Array[Item]],
                                            minCount: Long,
                                            partitioner: Partitioner): Array[(Item, Long)] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
  }

  /**
    * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
    * @param data transactions
    * @param minCount minimum count for frequent itemsets
    * @param freqItems frequent items
    * @param partitioner partitioner used to distribute transactions
    * @return an RDD of (frequent itemset, count)
    */
  private def genClosedItemsets[Item: ClassTag](
                                               data: RDD[Array[Item]],
                                               minCount: Long,
                                               freqItems: Array[Item],
                                               partitioner: Partitioner
                                               ):
                                                                          RDD[ClosedItemset[Item]] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    val closed_RDD = data
      .flatMap { transaction =>
        genCondTransactions(transaction, itemToRank, partitioner)
      }
      .aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
        (tree, transaction) => tree.add(transaction, 1L),
        (tree1, tree2) => tree1.merge(tree2))
      .flatMap { case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
      }
      .map {
        case (ranks, count) => (count, ranks.toSet)
      }
      .aggregateByKey(new CFIDS[Int], partitioner.numPartitions)(
        (map1, transaction) => map1.add(transaction),
        (map1, map2) => map1.merge(map2)
      )
      .flatMap {
        case (count, transactions) =>
          transactions.extract.map(x => (x, count))
      }
      .map { case (ranks, count) =>
          new ClosedItemset(ranks.map(i => freqItems(i)).toArray, count, Array[Long]())
    }
    if (minItems > 0){
      return closed_RDD.filter{x=> x.items.size >= minItems}
    }
    closed_RDD
  }

  /**
    * Generates conditional transactions.
    * @param transaction a transaction
    * @param itemToRank map from item to their rank
    * @param partitioner partitioner used to distribute transactions
    * @return a map of (target partition, conditional transaction)
    */
  private def genCondTransactions[Item: ClassTag](
                                                   transaction: Array[Item],
                                                   itemToRank: Map[Item, Int],
                                                   partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }

  /**
    * Generate row number for the closed item sets if gen row numbers is true
    */
  private def genRowNumbers[Item: ClassTag](data: RDD[Array[Item]],
                                            closed_RDD: RDD[ClosedItemset[Item]]): RDD[ClosedItemset[Item]] = {
    val dataWithIndex = data.zipWithIndex.map{case (v,k) => (k+1,v.toSet)}
    val closed = closed_RDD.map(c => (c.freq,c.items.toSet))

    val joined = closed.cartesian(dataWithIndex)
    joined.filter{x => x._1._2.subsetOf(x._2._2)}
        .map{x=> (x._1,x._2._1)}
        .groupByKey()
        .map{x=> new ClosedItemset(x._1._2.toArray,x._1._1,x._2)}

  }
}

@Since("1.3.0")
object FPClose {

  /**
    * Frequent itemset.
    * @param items items in this itemset. Java users should call `FreqItemset.javaItems` instead.
    * @param freq frequency
    * @tparam Item item type
    *
    */
  @Since("1.3.0")
  class ClosedItemset[Item] @Since("1.3.0")(
                                            @Since("1.3.0") val items: Array[Item],
                                            @Since("1.3.0") val freq: Long,
                                            @Since("FP Close Mod") val rowNumbers: Iterable[Long]
                                            ) extends Serializable {

    /**
      * Returns items in a Java List.
      *
      */
    @Since("2.4.0")
    def javaItems: java.util.List[Item] = {
      items.toList.asJava
    }

    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq ${rowNumbers.mkString("[", "," ,"]")}"
    }
  }


}