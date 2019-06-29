package org.apache.spark.mllib.fpm

import scala.collection.mutable.ListBuffer

private[fpm] class CFIMap[T] extends Serializable  {


  var closed_map = ListBuffer[Set[T]]()

  def add(itemset: Set[T]): this.type = {



    closed_map = closed_map.filterNot(x=> x.subsetOf(itemset))
//
//    var flag = 1
//    closed_map.foreach{ x=>q
//      if(itemset.subsetOf(x)){
//        flag = 0
//      }
//    }
//
//    if (flag == 1){
//      closed_map += itemset
//    }

    if (closed_map.indexWhere(x=> itemset.subsetOf(x)) == -1){
      closed_map += itemset
    }


    this

  }

  def merge(other: CFIMap[T]) : this.type = {

    other.closed_map.foreach{ item =>
      add(item)
    }


    this
  }







}
