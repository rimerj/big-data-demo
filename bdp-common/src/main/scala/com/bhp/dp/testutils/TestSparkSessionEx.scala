package com.bhp.dp.testutils

import ch.qos.logback.classic.Level
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object TestSparkSessionEx {
  def createSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[4]")
      .appName("BDPlocal")
      .config("spark.ui.enabled", value = false)
      .config("spark.driver.bindAddress", value = "127.0.0.1")
      .config("spark.executor.memory", "6g")
//      .config("spark.driver.memory", "3g")
      .config("spark.driver.host", "localhost")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.unsafe", "true")
      .config("spark.kryoserializer.buffer", "64k")
      .config("spark.serializer.objectStreamReset", "-1")
      .config("spark.broadcast.compress", "false")
      .config("spark.rdd.compress", "false")
      .config("spark.shuffle.compress", "false")
      // spark sql tweaks
      .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
      .config("spark.sql.optimizer.metadataOnly ", "false")
      .config("spark.sql.inMemoryColumnarStorage.compressed ", "false")
      .config("spark.sql.orc.compression.codec ", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.optimizer.maxIterations", "5")
      .getOrCreate()
  }

  private lazy val singletonSparkSession: SparkSession = createSparkSession

  private val sparkSessionInternal: ThreadLocal[SparkSession] = new ThreadLocal[SparkSession]()

  private def setLogLevels(level: Level, loggers: Seq[String]): Map[String, Level] = loggers.map(loggerName => {
    val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[ch.qos.logback.classic.Logger]
    val prevLevel = logger.getLevel
    logger.setLevel(level)
    loggerName -> prevLevel
  }).toMap

  def suppressSparkLogging() = {
     setLogLevels(Level.WARN, Seq("org.apache.spark", "org.apache.hadoop", "org.apache.parquet", "org.spark_project", "org.sparkproject", "org.eclipse.jetty", "akka", "io.netty"))
  }

  def sparkSession: SparkSession = this.synchronized {
    if (sparkSessionInternal.get == null) {
      suppressSparkLogging()
      createSession()
    }
    sparkSessionInternal.get
  }

  private def createSession(): Unit = {
    sparkSessionInternal.set(singletonSparkSession.newSession())
  }

}
