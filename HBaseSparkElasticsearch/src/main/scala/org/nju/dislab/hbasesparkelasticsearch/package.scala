package org.nju.dislab

/**
  * Created by streamer on 16-5-3.
  */
package object hbasesparkelasticsearch extends HBaseWriter
  with HBaseWriterTrait
  with HBaseReader
  with HBaseReaderTrait
  with HBaseDelete
  with HFileSupport
  with HBaseUtils
