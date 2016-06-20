package org.nju.dislab.streamer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.nju.dislab.hbasesparkelasticsearch._

import scala.collection.mutable.ArrayBuffer


object CreateTable extends App   {

  val name = "创建HBase数据表并加载协处理器"
  val master="spark://slave01:7077"
  //val master="spark://ubuntu:7077"
  lazy val sparkConf = new SparkConf().
    setAppName(name).
    setMaster(master).
    setSparkHome(System.getenv("SPARK_HOME"))
  lazy val sc = new SparkContext(sparkConf)

  //配置HBase连接参数
  implicit val config = HBaseConfig(
     "hbase.rootdir" -> "/opt/bigdata/hbase",
     "hbase.zookeeper.quorum" -> "slave05",
     "hbase.zookeeper.property.clientPort"->"2181"
   )

 /*   implicit val config = HBaseConfig(
     "hbase.rootdir" -> "/usr/local/PredictionIO/vendors/hbase-1.2.1",
     "hbase.zookeeper.quorum" -> "ubuntu",
     "hbase.zookeeper.property.clientPort"->"2181"
   )*/
  //HBase Admin
  val admin = Admin()
  //Column Family：固定的为GPSDATA
  val families = Set("GPSDATA")

  //获取HDFS上的文件名,根据文件名建表
  var tables = ArrayBuffer[String]()
  val fileSystem = FileSystem.get(new Configuration())
  //val filestatus = fileSystem.listStatus(new Path("/user/streamer/traffic/hbasedata"))
  val filestatus = fileSystem.listStatus(new Path("/user/hadoop/traffic/hbasedata"))
  filestatus.foreach(fs => tables+=fs.getPath.getName )

  val coprocessorInhdfs:Path = new Path("hdfs://slave01:9000/user/hadoop/COPROCESSOR/HBaseElasticsearchCoprocessor.jar")
  //val coprocessorInhdfs:Path = new Path("hdfs://ubuntu:9000/user/streamer/COPROCESSOR/HBaseElasticsearchUbuntu.jar")
  val className:String="org.streamer.dislab.HBaseElasticsearch.HBaseObserver"

  //val tables_pre = ArrayBuffer[String]("20160201","20160228","20160301","20160302","20160303","20160304","20160305")
  for (table <- tables){

    //建表,若表存在则删除
    //if (admin.tableExists(table,families)){
      //admin.disableTable(table)
      //admin.deleteTable(table)
    //}

    if(!admin.tableExists(table,families)){
      val hdfsdata = sc.textFile("/user/hadoop/traffic/hbasedata/"+table)
      val regionsCount=math.ceil(hdfsdata.count()*128/(256*1024*1024)).toInt
      val hdfskeys =hdfsdata.map(line => line.split(",")(0)).distinct()
      //计算区域
      val splitKeys: Seq[String]=computeSplits(hdfskeys,regionsCount)
      //建表,加载协处理器
      admin.createTable(table,families,splitKeys,className,coprocessorInhdfs)
      println(table+"表已经建好并加载了协处理器!")
    }

  }

  admin.close



}
