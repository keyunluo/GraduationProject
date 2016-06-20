package org.nju.dislab.hbasesparkelasticsearch

/**
  * Created by streamer on 16-5-3.
  */


import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.nju.dislab.hbasesparkelasticsearch.HBaseDeleteMethods._


trait HBaseDelete{

  implicit def deleteHBaseRDDKey(rdd: RDD[String]): HBaseDeleteRDDKey =
    new HBaseDeleteRDDKey(rdd, del)

  implicit def deleteHBaseRDDSimple(rdd: RDD[(String, Set[String])]): HBaseDeleteRDDSimple[String] =
    new HBaseDeleteRDDSimple(rdd, del)

  implicit def deleteHBaseRDDSimpleT(rdd: RDD[(String, Set[(String, Long)])]): HBaseDeleteRDDSimple[(String, Long)] =
    new HBaseDeleteRDDSimple(rdd, delT)

  implicit def deleteHBaseRDD(rdd: RDD[(String, Map[String, Set[String]])]): HBaseDeleteRDD[String] =
    new HBaseDeleteRDD(rdd, del)

  implicit def deleteHBaseRDDT(rdd: RDD[(String, Map[String, Set[(String, Long)]])]): HBaseDeleteRDD[(String, Long)] =
    new HBaseDeleteRDD(rdd, delT)
}

private[hbasesparkelasticsearch] object HBaseDeleteMethods {
  type Deleter[A] = (Delete, Array[Byte], A) => Delete

  // Delete
  def del(delete: Delete, cf: Array[Byte], q: String) = delete.addColumns(cf, q)
  def delT(delete: Delete, cf: Array[Byte], qt: (String, Long)) = delete.addColumn(cf, qt._1, qt._2)
}

sealed abstract class HBaseDeleteHelpers[A] {
  protected def convert(id: String, values: Map[String, Set[A]], del: Deleter[A]) = {
    val d = new Delete(id)
    var empty = true
    for {
      (family, contents) <- values
      content <- contents
    } {
      empty = false
      del(d, family, content)
    }

    if (empty) None else Some(new ImmutableBytesWritable, d)
  }

  protected def convert(id: String, families: Set[String]) = {
    val d = new Delete(id)
    for (family <- families) d.addFamily(family)
    Some(new ImmutableBytesWritable, d)
  }

  protected def convert(id: String) = {
    val d = new Delete(id)
    Some(new ImmutableBytesWritable, d)
  }
}

final class HBaseDeleteRDDKey(val rdd: RDD[String], val del: Deleter[String]) extends HBaseDeleteHelpers[String] with Serializable {

  def deleteHBase(table: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def deleteHBase(table: String, families: Set[String])(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, families) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def deleteHBase(table: String, family: String, columns: Set[String])(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, Map(family -> columns), del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def deleteHBase(table: String, qualifiers: Map[String, Set[String]])(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, qualifiers, del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseDeleteRDDSimple[A](val rdd: RDD[(String, Set[A])], val del: Deleter[A]) extends HBaseDeleteHelpers[A] with Serializable {

  def deleteHBase(table: String, family: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseDeleteRDD[A](val rdd: RDD[(String, Map[String, Set[A]])], val del: Deleter[A]) extends HBaseDeleteHelpers[A] with Serializable {

  def deleteHBase(table: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}