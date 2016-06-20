package org.nju.dislab.streamer

import org.apache.spark.{SparkConf, SparkContext}
import org.nju.dislab.hbasesparkelasticsearch._

/**
  * Created by streamer on 16-5-17.
  */
object WriteToHBase extends App{
  val name = "Spark写HBase测试"
  val master="spark://ubuntu:7077"

  lazy val sparkConf = new SparkConf().
    setAppName(name).
    setMaster(master).
    setSparkHome(System.getenv("SPARK_HOME"))
  lazy val sc = new SparkContext(sparkConf)
  //配置HBase连接参数
  implicit val config = HBaseConfig(
    "hbase.rootdir" -> "/usr/local/PredictionIO/vendors/hbase-1.2.1",
    "hbase.zookeeper.quorum" -> "ubuntu",
    "hbase.zookeeper.property.clientPort"->"2181"
  )
  //HBase Admin
  //val admin = Admin()
  //Column Family：固定的为GPSDATA
  //val families = Set("GPSDATA")
 // val table:String = "20160601"
  //val splitKeys:Seq[String]=Seq[String]()
  //val coprocessorInhdfs:Path = new Path("hdfs://ubuntu:9000/user/streamer/COPROCESSOR/HBaseElasticsearchUbuntu.jar")
 // val className:String="org.streamer.dislab.HBaseElasticsearch.HBaseObserver"
  //val hdfsdata = sc.textFile("/user/streamer/test/20160601")

  /*
  //建表,若表存在则删除
  if (admin.tableExists(table,families)){
    print("表已经存在，先删除!")
    admin.disableTable(table)
    admin.deleteTable(table)
  }
  admin.createTable(table,families,splitKeys,className,coprocessorInhdfs)
  print("表创建完毕，下面进行数据写测试")*/
  //val linecount=hdfsdata.count();
  //print("共有"+linecount+"行数据");

  sc.textFile("/user/streamer/test/20160601").map({ line =>
      val Array(k, col1, col2, col3) = line split ","
      val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
      k -> content
    }).toHBase("20160601","GPSDATA")
  println("表20160601已经生成索引")

  /*
  sc.textFile("/user/hadoop/traffic/hbasedata/20160302")
    .map({ line =>
      val Array(k, col1, col2, col3) = line split ","
      val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
      k -> content
    }).toHBase("20160302","GPSDATA")
  println("表20160302已经生成索引")
  sc.textFile("/user/hadoop/traffic/hbasedata/20160303")
    .map({ line =>
      val Array(k, col1, col2, col3) = line split ","
      val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
      k -> content
    }).toHBase("20160303","GPSDATA")
  println("表20160303已经生成索引")
  sc.textFile("/user/hadoop/traffic/hbasedata/20160228")
    .map({ line =>
      val Array(k, col1, col2, col3) = line split ","
      val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
      k -> content
    }).toHBase("20160228","GPSDATA")
  println("表20160228已经生成索引")
  sc.textFile("/user/hadoop/traffic/hbasedata/20160304")
    .map({ line =>
      val Array(k, col1, col2, col3) = line split ","
      val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
      k -> content
    }).toHBase("20160304","GPSDATA")
  println("表20160304已经生成索引")
  sc.textFile("/user/hadoop/traffic/hbasedata/20160305")
    .map({ line =>
      val Array(k, col1, col2, col3) = line split ","
      val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
      k -> content
    }).toHBase("20160305","GPSDATA")
  println("表20160305已经生成索引")

  sc.textFile("/user/hadoop/traffic/hbasedata/20160201")
    .map({ line =>
      val Array(k, col1, col2, col3) = line split ","
      val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
      k -> content
    }).toHBase("20160201","GPSDATA")
  println("表20160201已经生成索引")
  */
}
