package com.bfd.relation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by d on 2016/11/29.
  */
object Test {
  def main(args: Array[String]) {
    //    PropertyConfigurator.configure("E:\\ideaProjects\\spark_test\\src\\resources\\log4j.properties")
    System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");
    val conf = new SparkConf().setMaster("spark://172.18.1.123:7077").setAppName("test").setJars(List("E:\\ideaProjects\\spark_test\\out\\artifacts\\spark_test_jar\\spark_test.jar"))
    val sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 9, 3)
//    val b = a.mapPartitions{x => {
//      var result = List()
//      var i = 0
//      while (x.hasNext) {
//        i += x.next()
//      }
//      result.::(i).iterator
//    }}
//    val c = b.reduce(_ + _)
//    println(c)
    val b = a.map(_*2).map(_/2).foreach(println(_))
  }
}
