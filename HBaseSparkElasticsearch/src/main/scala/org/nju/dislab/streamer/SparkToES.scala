package org.nju.dislab.streamer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

import scala.collection.mutable.ArrayBuffer




object SparkToES extends App {

  val name = "Spark生成索引至Elasticsearch中"
  //val master="spark://ubuntu:7077"
  val master="spark://slave01:7077"

  lazy val sparkConf = new SparkConf().
    setAppName(name).
    setMaster(master).
    setSparkHome(System.getenv("SPARK_HOME"))
  sparkConf.set("es.index.auto.create","true")
  sparkConf.set("es.nodes","slave01")
  //sparkConf.set("es.nodes","ubuntu")
  sparkConf.set("es.port","9200")
  //sparkConf.set("es.resource","gps_data/gps_type")
  //sparkConf.set("spark.kryoserializer.buffer.max","2000m")
  lazy val sc = new SparkContext(sparkConf)


  //获取HDFS上的文件名,根据文件名建表
  var tables = ArrayBuffer[String]()
  val fileSystem = FileSystem.get(new Configuration())
  val filestatus = fileSystem.listStatus(new Path("/user/hadoop/traffic/hbasedata"))
    //val filestatus = fileSystem.listStatus(new Path("/user/hadoop/traffic/TestResultData"))
  filestatus.foreach(fs => tables+=fs.getPath.getName )

  //获得HDFS上的公交车信息
  val busRDD=sc.textFile("/user/hadoop/traffic/businfo.csv").map(
    {
      line => val Array(k,col1,col2) = line split ","
        (col2 , col1)
    }
  )
  val busArray=busRDD.distinct().collect()
  val busMap= Map.empty ++ busArray
  val busBroadCast=sc.broadcast(busMap)

  for (table <- tables){
    val hdfsdata = sc.textFile("/user/hadoop/traffic/hbasedata/"+table)
    val indextype="gps_data/"+table

    val hdfscontent = hdfsdata.map({line =>val Array(k, col1, col2, col3) = line split ","
      val col =(k,col1)
      (k.substring(0,10),col)}).join(busRDD).map({
      line => val Array(c1,c2,c3,c4)=line.toString.replace("(","").replace(")","").split(",")
        val zdbh = c1
        val key = c2
        val gpssj =c3
        val xlmc = c4
        val content = Map("ZDBH" -> zdbh, "ROWKEY" -> key, "GPSSJ" -> gpssj,"XLMC" -> xlmc)
        (key,content)
    })

 /*   val hdfscontent = hdfsdata.map({line =>val Array(k, col1, col2, col3) = line split ","
      (k,col1)}).mapPartitions({ iter =>
      val m = busBroadCast.value
      for {
        (key, value) <- iter
        k= key.split(" ")(0)
        if (m.contains(k))
      } yield (key, Map("ZDBH" -> k, "ROWKEY" -> key, "GPSSJ" -> value,"XLMC" -> m.getOrElse(k,"NotFound")))
    })*/

  /*  val hdfscontent =hdfsdata.map({line => val Array(k, col1, col2, col3) = line split ","
      val zdbh=k.split(" ")(0).toString
      val xlmc=busBroadCast.value.getOrElse(zdbh,"NotFound")
      val content = Map("ZDBH" -> zdbh, "ROWKEY" -> k, "GPSSJ" -> col1,"XLMC" -> xlmc)
      (k,content)
    }
    )*/
    println("=====共有"+hdfscontent.count()+"行数据需要写入ES中=====")
    hdfscontent.saveToEsWithMeta(indextype)

  }
}