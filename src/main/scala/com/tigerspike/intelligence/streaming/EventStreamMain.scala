package com.jetri.intelligence.streaming

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, Seconds}

object EventStreamMain {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println(
        """
          |Usage: IntelligenceEventStreamProcessor <projectId> <batchInterval> <elasticsearchURL>
          |
          |    <batchInterval> is the interval in seconds in which data is pulled from Kinesis and sent to Spark applications receiver
          |    <repartitionCount> is the number of repartition for the aggregated dataframe. this will have an impact on the number of Spark tasks
          |    <elasticsearchURL> is the URL to the AWS Elasticsearch domain within the same VPC
          |    <sparkcheckpointDirectory> is the directory where Spark will save it's checkpoint data
        """.stripMargin)
      System.exit(1)
    }

    val Array(batchIntervalParam, repartitionCountParam, elasticSearchURL, checkPointDirectory) = args
    val streamName = "Phoenix_LiveSG_EventStream"
    val regionName = "ap-southeast-1"
    val batchInterval = Seconds(batchIntervalParam.toInt)
    val checkpointInterval = Seconds(1)
    val initialPosition = InitialPositionInStream.LATEST
    val repartitionCount = repartitionCountParam.toInt

    val appName = s"IntelligenceSparkEventStreamProcessor-emr-dev"

    // This is default, and is normally used if you're running on your local machine for testing.
    // This should be overwritten in production.  Please refer to readme.
    // val master = "local[*]"

    val streamingConfig = StreamingConfig(streamName, regionName, checkpointInterval, initialPosition, StorageLevel.MEMORY_AND_DISK_2, appName, batchInterval, repartitionCount, elasticSearchURL, checkPointDirectory)

    EventStreamProcessor.execute(streamingConfig)
  }
}
