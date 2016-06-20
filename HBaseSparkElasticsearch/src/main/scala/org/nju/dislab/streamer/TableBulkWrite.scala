package org.nju.dislab.streamer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.nju.dislab.hbasesparkelasticsearch._

import scala.collection.mutable.ArrayBuffer


object TableBulkWrite {

  val name = "创建HBase数据表并加载协处理器,Bulk写到HBase"
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

  val admin = Admin()
  //Column Family：固定的为GPSDATA
  val families = Set("GPSDATA")

  //获取HDFS上的文件名,根据文件名建表
  var tables = ArrayBuffer[String]()
  val fileSystem = FileSystem.get(new Configuration())
  val filestatus = fileSystem.listStatus(new Path("/user/hadoop/traffic/hbasedata"))
  filestatus.foreach(fs => tables+=fs.getPath.getName )

  val coprocessorInhdfs:Path = new Path("hdfs://slave01:9000/user/hadoop/COPROCESSOR/HBaseElasticsearchCoprocessor.jar")
  val className:String="org.streamer.dislab.HBaseElasticsearch.HBaseObserver"

  for (table <- tables){
    if(!admin.tableExists(table,families)){
      val hdfsdata = sc.textFile("/user/hadoop/traffic/hbasedata/"+table)
      val regionsCount=math.ceil(hdfsdata.count()*128/(256*1024*1024)).toInt
      val hdfskeys =hdfsdata.map(line => line.split(",")(0)).distinct()
      //计算区域
      val splitKeys: Seq[String]=computeSplits(hdfskeys,regionsCount)
      //建表,加载协处理器
      admin.createTable(table,families,splitKeys,className,coprocessorInhdfs)
      println(table+"表已经建好并加载了协处理器!")
      hdfsdata.map({ line =>
          val Array(k, col1, col2, col3) = line split ","
          val content = Map("GPSSJ" -> col1, "JD" -> col2, "WD" -> col3)
          k -> content
        }).
        toHBaseBulk(table,"GPSDATA")
      println("表"+table+"导入成功")
    }

  }

  admin.close

}
