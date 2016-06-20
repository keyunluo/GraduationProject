package org.nju.dislab.hbasesparkelasticsearch


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * HBaseConfiguration封装类
  */
class HBaseConfig(defaults: Configuration) extends Serializable {
  def get = HBaseConfiguration.create(defaults)
}

/**
  * 单例对象：
  */
object HBaseConfig {
  def apply(conf: Configuration): HBaseConfig = new HBaseConfig(conf)

  def apply(options: (String, String)*): HBaseConfig = {
    val conf = HBaseConfiguration.create
    for ((key, value) <- options) {
      conf.set(key, value)
    }
    apply(conf)
  }

  def apply(conf: { def rootdir: String; def quorum: String }): HBaseConfig = apply(
    "hbase.rootdir" -> conf.rootdir,
    "hbase.zookeeper.quorum" -> conf.quorum)
}

