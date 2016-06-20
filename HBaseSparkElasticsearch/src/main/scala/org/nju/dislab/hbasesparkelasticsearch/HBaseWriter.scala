package org.nju.dislab.hbasesparkelasticsearch

/**
  * Created by streamer on 16-5-3.
  */


import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.nju.dislab.hbasesparkelasticsearch.HBaseWriteMethods._


trait HBaseWriter {

  implicit def toHBaseRDDSimple[A](rdd: RDD[(String, Map[String, A])])(implicit writer: Writes[A]): HBaseWriteRDDSimple[A] =
    new HBaseWriteRDDSimple(rdd, pa[A])

  implicit def toHBaseRDDSimpleTS[A](rdd: RDD[(String, Map[String, (A, Long)])])(implicit writer: Writes[A]): HBaseWriteRDDSimple[(A, Long)] =
    new HBaseWriteRDDSimple(rdd, pa[A])

  implicit def toHBaseRDDFixed[A](rdd: RDD[(String, Seq[A])])(implicit writer: Writes[A]): HBaseWriteRDDFixed[A] =
    new HBaseWriteRDDFixed(rdd, pa[A])

  implicit def toHBaseRDDFixedTS[A](rdd: RDD[(String, Seq[(A, Long)])])(implicit writer: Writes[A]): HBaseWriteRDDFixed[(A, Long)] =
    new HBaseWriteRDDFixed(rdd, pa[A])

  implicit def toHBaseRDD[A](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: Writes[A]): HBaseWriteRDD[A] =
    new HBaseWriteRDD(rdd, pa[A])

  implicit def toHBaseRDDT[A](rdd: RDD[(String, Map[String, Map[String, (A, Long)]])])(implicit writer: Writes[A]): HBaseWriteRDD[(A, Long)] =
    new HBaseWriteRDD(rdd, pa[A])
}

private[hbasesparkelasticsearch] object HBaseWriteMethods {
  type PutAdder[A] = (Put, Array[Byte], Array[Byte], A) => Put

  // PutAdder
  def pa[A](put: Put, cf: Array[Byte], q: Array[Byte], v: A)(implicit writer: Writes[A]) = put.addColumn(cf, q, writer.write(v))
  def pa[A](put: Put, cf: Array[Byte], q: Array[Byte], v: (A, Long))(implicit writer: Writes[A]) = put.addColumn(cf, q, v._2, writer.write(v._1))
}

sealed abstract class HBaseWriteHelpers[A] {
  protected def convert(id: String, values: Map[String, Map[String, A]], put: PutAdder[A]) = {
    val p = new Put(id)
    var empty = true
    for {
      (family, content) <- values
      (key, value) <- content
    } {
      empty = false
      put(p, family, key, value)
    }

    if (empty) None else Some(new ImmutableBytesWritable, p)
  }
}

final class HBaseWriteRDDSimple[A](val rdd: RDD[(String, Map[String, A])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {

  def toHBase(table: String, family: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseWriteRDDFixed[A](val rdd: RDD[(String, Seq[A])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {

  def toHBase(table: String, family: String, headers: Seq[String])(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    val sc = rdd.context
    val bheaders = sc.broadcast(headers)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> Map(bheaders.value zip v: _*)), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseWriteRDD[A](val rdd: RDD[(String, Map[String, Map[String, A]])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {

  def toHBase(table: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
