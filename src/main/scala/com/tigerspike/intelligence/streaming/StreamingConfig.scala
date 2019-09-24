package com.jetri.intelligence.streaming

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration

case class StreamingConfig(
streamName:         String,
region:             String,
checkpointInterval: Duration,
initialPosition:    InitialPositionInStream,
storageLevel:       StorageLevel,
appName:            String,
batchInterval:      Duration,
repartitionCount:   Int,
elasticSearchUrl:   String,
checkPointDirectory: String) {
  val kinesisEndpointUrl = s"https://kinesis.${region}.amazonaws.com"
}
