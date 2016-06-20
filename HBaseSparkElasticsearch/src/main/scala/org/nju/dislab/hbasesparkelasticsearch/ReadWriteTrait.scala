package org.nju.dislab.hbasesparkelasticsearch

/**
  * Created by streamer on 16-5-1.
  * 读写Trait
  */
trait Reads[A] extends Serializable {
  def read(data: Array[Byte]): A
}

trait Writes[A] extends Serializable {
  def write(data: A): Array[Byte]
}

trait ReadWriteTrait[A] extends Reads[A] with Writes[A]