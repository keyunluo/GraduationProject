package org.nju.dislab.hbasesparkelasticsearch

/**
  * Created by streamer on 16-5-4.
  */

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Coprocessor, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

trait HBaseUtils {

  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def arrayToBytes(a: Array[String]): Array[Array[Byte]] = a map Bytes.toBytes

  protected[hbasesparkelasticsearch] def createJob(table: String, conf: Configuration) = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job
  }

  object Admin {
    def apply()(implicit config: HBaseConfig) = new Admin(ConnectionFactory.createConnection(config.get))
  }

  class Admin(connection: Connection) {

    def close = connection.close()


    def tableExists(tableName: String, family: String): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        val families = admin.getTableDescriptor(table).getFamiliesKeys
        require(families.contains(family.getBytes), s"Table [$table] exists but column family [$family] is missing")
        true
      } else false
    }


    def tableExists(tableName: String, families: Set[String]): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        val tfamilies = admin.getTableDescriptor(table).getFamiliesKeys
        for (family <- families)
          require(tfamilies.contains(family.getBytes), s"Table [$table] exists but column family [$family] is missing")
        true
      } else false
    }


    def snapshot(tableName: String): Admin = {
      val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
      val suffix = sdf.format(Calendar.getInstance().getTime)
      snapshot(tableName, s"${tableName}_$suffix")
      this
    }


    def snapshot(tableName: String, snapshotName: String): Admin = {
      val admin = connection.getAdmin
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      admin.snapshot(snapshotName, tableDescriptor.getTableName)
      this
    }


    def createTable(tableName: String, families: Set[String], splitKeys: Seq[String]): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (!admin.isTableAvailable(table)) {
        val tableDescriptor = new HTableDescriptor(table)
        families foreach { f => tableDescriptor.addFamily(new HColumnDescriptor(f)) }
        if (splitKeys.isEmpty)
          admin.createTable(tableDescriptor)
        else {
          val splitKeysBytes = splitKeys.map(Bytes.toBytes).toArray
          admin.createTable(tableDescriptor, splitKeysBytes)
        }
      }
      this
    }

    def createTable(tableName: String, families: Set[String], splitKeys: Seq[String],className:String,pathinhdfs:Path): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (!admin.isTableAvailable(table)) {
        val tableDescriptor = new HTableDescriptor(table)
        families foreach { f => tableDescriptor.addFamily(new HColumnDescriptor(f)) }
        tableDescriptor.addCoprocessor(className,pathinhdfs,Coprocessor.PRIORITY_USER,null)
        if (splitKeys.isEmpty)
          admin.createTable(tableDescriptor)
        else {
          val splitKeysBytes = splitKeys.map(Bytes.toBytes).toArray
          admin.createTable(tableDescriptor, splitKeysBytes)
        }
      }
      this
    }

    def createTable(tableName: String, families: Set[String]): Admin =
      createTable(tableName, families, Seq.empty)


    def createTable(tableName: String, families: String*): Admin =
      createTable(tableName, families.toSet, Seq.empty)


    def createTable(tableName: String, family: String, splitKeys: Seq[String]): Admin =
      createTable(tableName, Set(family), splitKeys)

    def disableTable(tableName: String): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.disableTable(table)
      this
    }

    def deleteTable(tableName: String): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.deleteTable(table)
      this
    }

    def truncateTable(tableName: String, preserveSplits: Boolean): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.truncateTable(table, preserveSplits)
      this
    }

  }


  def computeSplits(rdd: RDD[String], regionsCount: Int): Seq[String] = {
    rdd.sortBy(s => s, numPartitions = regionsCount)
      .mapPartitions(_.take(1))
      .collect().toList.tail
  }
}
