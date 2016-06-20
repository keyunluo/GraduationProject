package org.nju.dislab.streamer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.nju.dislab.hbasesparkelasticsearch._

import scala.collection.mutable.ArrayBuffer


object BulkWrite extends App {
  val name = "使用HFiles进行Bulk Load写HBase"
  val master="spark://slave01:7077"

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
  /*implicit val config = HBaseConfig(
    "hbase.rootdir" -> "/usr/local/PredictionIO/vendors/hbase-1.2.1",
    "hbase.zookeeper.quorum" -> "ubuntu",
    "hbase.zookeeper.property.clientPort"->"2181"
  )*/
  val admin = Admin()

  //获取HDFS上的文件名,根据文件名建表
  var tables = ArrayBuffer[String]()
  val fileSystem = FileSystem.get(new Configuration())
  val filestatus = fileSystem.listStatus(new Path("/user/hadoop/traffic/hbasedata"))
  filestatus.foreach(fs => tables+=fs.getPath.getName )

  for (table <- tables){
    if (admin.tableExists(table,"GPSDATA")){
      sc.textFile("/user/hadoop/traffic/hbasedata/"+table).
        map({ line =>
          val Array(k, col1, col2, col3) = line split ","
          val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
          k -> content
        }).
        toHBaseBulk(table,"GPSDATA")
      println("表"+table+"导入成功")
    }
    else {
      println("表不存在，无法导入!")
    }
  }
  admin.close

}
