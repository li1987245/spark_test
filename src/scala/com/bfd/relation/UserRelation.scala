package com.bfd.relation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by d on 2016/11/29.
  */
object UserRelation {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");
    val conf = new SparkConf().setMaster("spark://172.18.1.123:7077").setAppName("user_relation"+System.currentTimeMillis())
   conf.setJars(List("E:\\ideaProjects\\spark_test\\out\\artifacts\\spark_test_jar\\spark_test.jar"))
//   conf.set("spark.cores.max","8").set("spark.executor.cores","2").set("spark.driver.memory", "1g")
    conf.set("spark.cores.max","8").set("spark.executor.cores","2").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("hdfs://tb39-nn01:8020/user/hive/warehouse/xniddata.db/user_relation/l_date=2016-10-14/000000_0")
    //[{"id":"5fedae27b9de4804869d4cce8168a654","id_type":"gid","channel":"global"},{"id":"28lvZk91Hb3H","id_type":"mac","channel":"global"},{"id":"7947125@em.com","id_type":"em","channel":"global"}]
    val tmp = textFile.map(_.split("\t")(1)).foreach(println(_))
    //    val tmp = textFile.map(_.split("\t")(1)).map(str =>{
//      val arr = JSON.parseFull(str)
//      arr match {
//        case Some(lst: List[String]) => println(lst)
//        case Some(map: Map[String, Any]) => println(map)
//        case None => println("Parsing failed")
//        case other => println("Unknown data structure: " + other)
//      }
//    })
  }
}
