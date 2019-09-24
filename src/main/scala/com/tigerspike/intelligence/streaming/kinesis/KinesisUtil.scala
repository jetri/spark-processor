package com.jetri.intelligence.streaming.kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient

object KinesisUtil {
  def getShardCount(kinesisClient: AmazonKinesisClient, stream: String): Int =
    kinesisClient
      .describeStream(stream)
      .getStreamDescription
      .getShards
      .size

  /**
    * Finds AWS Credential by provided awsProfile and creates Kinesis Client
    */
  def setupKinesisClientConnection(endpointUrl: String): AmazonKinesisClient = {
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    val akc = new AmazonKinesisClient(credentials)
    akc.setEndpoint(endpointUrl)
    akc
  }
}
