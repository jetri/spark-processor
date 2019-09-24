package com.jetri.intelligence.streaming

import java.util.{Calendar, Date, TimeZone}

import com.sanoma.cda.geo.Point
import com.sanoma.cda.geoip.MaxMindIpGeo
import com.jetri.intelligence.streaming.kinesis.KinesisUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{KinesisInputDStream, KinesisUtils}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.json4s.JsonAST.{JNothing, JNull, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, JArray, JInt, JObject, JString}
import org.elasticsearch.spark.sql._
import java.util.Calendar
import java.text.SimpleDateFormat

import scala.util.Try

object EventStreamProcessor {
  val aggregationMap: Map[String, Column => Column] = Map("min" -> min, "max" -> max, "avg" -> avg, "sum" -> sum, "count" -> count, "approx_count_distinct" -> approx_count_distinct)

  def serializeList(test: Map[String, String]): JValue ={
    implicit val formats = DefaultFormats
    parse(Serialization.write(test))
  }

  def convertJArrayToList(test: JValue): Option[List[Map[String, String]]] ={
    implicit val formats = DefaultFormats
    val list = Try{test.values.asInstanceOf[List[Map[String, String]]]}.toOption
    if (list == None){
      None
    } else {
      list
    }
  }

  def createStreamingContext(sparkContext: SparkContext, config: StreamingConfig, sparkConf: SparkConf): StreamingContext = {
    val sparkStreamingContext = new StreamingContext(sparkContext, config.batchInterval)

    val kinesisClient = KinesisUtil.setupKinesisClientConnection(config.kinesisEndpointUrl)
    val numShards = KinesisUtil.getShardCount(kinesisClient, config.streamName)

    println("Create the DStreams")
    val kinesisStreams = (0 until numShards).map { i =>
      KinesisUtils.createStream(
        ssc = sparkStreamingContext,
        kinesisAppName = config.appName,
        streamName = config.streamName,
        endpointUrl = config.kinesisEndpointUrl,
        regionName = config.region,
        initialPositionInStream = config.initialPosition,
        checkpointInterval = config.checkpointInterval,
        storageLevel = config.storageLevel)
    }

    println("Union all the streams")
    val unionStreams = sparkStreamingContext.union(kinesisStreams)
    val eventsDstream = unionStreams.map(eventByteArray => getEventJson(eventByteArray)).flatMap(x => convertJArrayToList(x)).flatMap(b => b).map(l => serializeList(l))
    val enrichedEvents = eventsDstream.map(x => enrichEventsWithGeohash(x))
    val inferredMetdataTypes = enrichedEvents.map(x => inferMetadataFieldsType(x))

    println("Start")
    inferredMetdataTypes.foreachRDD((eventsRDD, time) => {
      val sqlContext = SQLContextSingleton.getInstance(eventsRDD.sparkContext)
      import sqlContext.implicits._

      val eventsRDDString = eventsRDD.map(e => compact(render(e)))
      eventsRDDString.cache()

      val projects = getProjects()
      projects.foreach(project => {

        val projectId = project.projectId.toInt
        val projectRDD = getProjectRDD(projectId, eventsRDDString)

        if (!projectRDD.isEmpty()) {

          val eventsDF = getDataFrameFromProjectEventsRDD(project.projectId, projectRDD)
          val windowInterval = window(unix_timestamp($"EventDate", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp"), "5 minutes")
          val flattenedDF = eventsDF.flattenSchema
          val finalDFToProcess = flattenedDF.toDF(flattenedDF.columns.map(_.replace(".", "_")): _*)
            .withColumn("EventDate", windowInterval("start"))
            .drop("Geolocation")
            .drop("IpAddress")
            .drop("ProcessorDate")
            .drop("ProcessorLatency")
            .drop("PhoenixIdentity_InstallationId")
            .drop("PhoenixIdentity_ApplicationId")
            .drop("Timestamp")

          val projectMetricNames = project.metrics.map(x => x.fieldName)

          // Exclude known default event columns that are approx count metrics and fields that are numeric but are not required to be aggregated.
          // Exclude known project metrics as well
          val excludeForCols = "PhoenixIdentity_UserId" :: "ProjectId" :: "TargetId" :: projectMetricNames.map(x => s"Metadata_$x")

          val metricColNames = getMetricColNames(finalDFToProcess, excludeForCols)
          val metricCols = metricColNames.map(x => col(s"$x"))
          val metricNamesCombined = projectMetricNames :: metricColNames :: Nil

          val excludeForDimensionsCol = excludeForCols :: metricNamesCombined.flatMap(x => x) :: Nil
          val dimensions = getDimensionCols(finalDFToProcess, excludeForDimensionsCol.flatMap(x => x))

          val dateFormat = new SimpleDateFormat("yyyyMMdd")
          val calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

          // Get aggregation columns for default and project metrics
          val aggMetricCols = getAggMetricCols(metricCols)
          val aggProjectMetricCols = getAggProjectMetricCols(project.metrics)

          val aggMetricsCombinedList = aggMetricCols :: aggProjectMetricCols :: Nil
          val aggMetrics = aggMetricsCombinedList.flatMap(x => x)

          // Note: watermarking for some reason don't work with local
//          val aggregatedEvents = finalDFToProcess.groupBy(dimensions: _*).agg(aggMetrics.head, aggMetrics.tail: _*)
//          println("phoenixevent-livesg-" + projectId + "-" + dateFormat.format(calendar.getTime) + "/phoenixevent")
//          aggregatedEvents.printSchema()
//          aggregatedEvents.show(20)

          val aggregatedEvents = finalDFToProcess.withWatermark("EventDateTimestamp", "5 minutes").groupBy(dimensions: _*).agg(aggMetrics.head, aggMetrics.tail: _*)
          val esResource = "phoenixevent-livesg-" + projectId + "-" + dateFormat.format(calendar.getTime) + "/phoenixevent"

          if (config.repartitionCount == 0) {
            aggregatedEvents.saveToEs(esResource)
          } else {
            // In general, you can determine the number of partitions by multiplying the number of CPUs in the cluster by 2, 3, or 4.
            // https://stackoverflow.com/questions/35800795/number-of-partitions-in-rdd-and-performance-in-spark/35804407#35804407
            // http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
            aggregatedEvents.repartition(config.repartitionCount).saveToEs(esResource)
          }
        }
      })
    })

    sparkStreamingContext.checkpoint(config.checkPointDirectory)
    sparkStreamingContext
  }

  def setSparkConfig(config: StreamingConfig): SparkConf = {
    val sparkConf = new SparkConf().setAppName(config.appName)

    // Spark

    // Allow Spark to create executors and allocate executor cores when necessary
    sparkConf.set("spark.dynamicAllocation.enabled", "true")

    // Set minimum executors to 10, creating executors take some time so 8 is a good number
    sparkConf.set("spark.dynamicAllocation.minExecutors", "8")

    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Spark Streaming

    // The lower this is, the more partitions/tasks gets created, meaning the more tasks may be distributed to nodes (more parallelism)
    sparkConf.set("spark.streaming.blockInterval", "100ms")

    // The max number of events to be received/processed per second.  This helps with sudden surges in data coming in and when recovering from checkpoints as well
    sparkConf.set("spark.streaming.receiver.maxRate", "90")

    // ElasticSearch
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes.wan.only", "true")
    sparkConf.set("es.port", "80")
    sparkConf.set("es.nodes", config.elasticSearchUrl)

    sparkConf
  }

  def execute(config: StreamingConfig) {
    val sparkConf = setSparkConfig(config)

    val sparkContext = new SparkContext(sparkConf)
    val hadoopConf = sparkContext.hadoopConfiguration
    val secretAccessKey = ""

    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3a.awsAccessKeyId", "")
    hadoopConf.set("fs.s3a.awsSecretAccessKey", secretAccessKey)

    sparkContext.addFile("s3a://intelligence-event-processing-dev/GeoLite2-City.mmdb")

    // Start from Spark's checkpoint if exists
    val sparkStreamingContext = StreamingContext.getOrCreate(config.checkPointDirectory, () => {
      createStreamingContext(sparkContext, config, sparkConf)
    })

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  /**
    * Converts byte array Intelligence events to json strings.
    * @param byteArray e.g. from Kinesis DStream
    * @return JSON string
    */
  def getEventJson(byteArray: Array[Byte]) : JValue = {
    val jsonString = new String(byteArray)
    val jval = parse(jsonString)
    val jValData = jval \ "data"

    if (jValData == JNull | jValData == JNothing) {
      val b = jval
      return b
    }
    else {
      val a = jValData
      return a
    }
  }

  var schemaMap = scala.collection.mutable.Map[String, StructType]()
  var schemaMapRefreshDay = getNextDay()

  def getNextDay(): Calendar ={
    val nextDay = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

    nextDay.add(Calendar.DAY_OF_MONTH, 1)
    nextDay.set(Calendar.HOUR_OF_DAY, 0)
    nextDay.set(Calendar.MINUTE, 0)
    nextDay.set(Calendar.SECOND, 0)
    nextDay.set(Calendar.MILLISECOND, 0)

    nextDay
  }

  def getDataFrameFromProjectEventsRDD(projectId: String, rdd: RDD[String]) : DataFrame = {
    val sqlContext = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate().sqlContext
    val nextDayInMillis = schemaMapRefreshDay.getTimeInMillis()
    val currentDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    val diff = nextDayInMillis - currentDate.getTimeInMillis

    // Clear the schema cache only on the next day
    if (diff <= 0){
      println("Clearing Schema Cache")

      schemaMap.clear()
      schemaMapRefreshDay = getNextDay()
    }

    // update in once a day
    if (!schemaMap.keySet.exists(_ == projectId)){
      val dataFrame = sqlContext.read.json(rdd)

      schemaMap(projectId) = dataFrame.schema

      println("Created or Updated Schema Cache: " + projectId + " - " + currentDate.getTime)
      dataFrame
    } else {
      val schema = schemaMap(projectId)
      sqlContext.read.schema(schema).json(rdd)
    }
  }

  /**
    * Creates Geo hash based on geo location or IP address from Event data
    * @param jVal
    * @return
    */
  def enrichEventsWithGeohash(jVal: JValue) : JValue = {
    implicit val formats = DefaultFormats
    val geolocation = jVal \ "Geolocation"
    var lon = geolocation \ "Longitude"
    var lat = geolocation \ "Latitude"

    val dbFile = SparkFiles.get("GeoLite2-City.mmdb")
    val maxMindGeoIp = MaxMindIpGeo(dbFile =  dbFile, lruCache = 10000, synchronized = true)

    if ((lon != JNull && lon != JNothing) && (lat != JNull && lat != JNothing)){
      val geoHash = Point(lat.extract[Double], lon.extract[Double]).geoHash(6) // We can potentially reduce or increase the precision here
      val updatedJval = jVal merge JObject("Geohash" -> JString(geoHash)) merge JObject("Geolocation" -> JArray(lon :: lat :: Nil))
      return updatedJval
    }

    val ipAddress = jVal \ "IPAddress"
    if (ipAddress != JNull | ipAddress != JNothing) {
      val maxMindGeoLocation = maxMindGeoIp.getLocation(ipAddress.extract[String])

      if (maxMindGeoLocation != None) {
        val maxMindGeoPoint = maxMindGeoLocation.head.geoPoint

        if (maxMindGeoPoint != None) {
          val maxMindGeoHash = maxMindGeoPoint.head.geoHash(6) // We can potentially reduce or increase the precision here
          val maxMindUpdated = jVal merge JObject("Geohash" -> JString(maxMindGeoHash)) merge JObject("Geolocation" -> JArray(lon :: lat :: Nil))

          return maxMindUpdated
        }
      }
    }

    // Nothing to do, just return empty geohash
    return jVal merge JObject("Geohash" -> JNull)
  }

  /**
    * Infer the datatype for Metdata fields.  This may no longer be needed if the Metadata fields are not converted to strings at the source.s
    * @param jVal
    * @return
    */
  def inferMetadataFieldsType(jVal: JValue) : JValue = {
    implicit val formats = DefaultFormats
    val metadata = jVal \ "Metadata"

    if (metadata == JNull | metadata == JNothing){
      return jVal
    }

    val metadataList = metadata.extract[Map[String, String]].map(d => {
      val newVal = Try(d._2.toDouble).toOption

      if (newVal != None) {
        Tuple2(d._1, newVal.get)
      } else {
        Tuple2(d._1, d._2)
      }
    })

    val serialised = parse(Serialization.write(metadataList))
    val metadataInferred = jVal merge JObject("Metadata" -> serialised)

    metadataInferred
  }

  def getProjectRDD(projectId: Int, rawRDD: RDD[String]): RDD[String] = {
    rawRDD.filter(x => {
      val json = parse(x)
      val id = json \ "ProjectId"

      if (id != JNothing && id == JInt(projectId)) {
        true
      }
      else{
        false
      }
    })
  }

  def getProjects(): List[Project] = {
    implicit val formats = DefaultFormats

    val rawJson = """[{"dimensions":[],"metrics":[{"fieldName":"DeviceId","fieldType":"","aggregationType":"approx_count_distinct"},{"fieldName":"IpAddress","fieldType":"","aggregationType":"approx_count_distinct"}],"projectId":14106,"projectName":"StraitsTimes"},{"dimensions":[],"metrics":[],"projectId":30024,"projectName":"DailyDough"},{"dimensions":[],"metrics":[],"projectId":10083,"projectName":"Maxis"},{"dimensions":[],"metrics":[],"projectId":30130,"projectName":"CMS"},{"dimensions":[],"metrics":[],"projectId":30102,"projectName":"Dashboard"},{"dimensions":[],"metrics":[],"projectId":30120,"projectName":"AFViOS"},{"dimensions":[],"metrics":[],"projectId":30103,"projectName":"DashboardStaging"},{"dimensions":[],"metrics":[],"projectId":30194,"projectName":"bKashUat"}]"""

//    val rawJson = """[{"dimensions":[],"metrics":[{"fieldName":"DeviceId","fieldType":"","aggregationType":"approx_count_distinct"},{"fieldName":"IpAddress","fieldType":"","aggregationType":"approx_count_distinct"}],"projectId":14106,"projectName":"Straits Times project"}]"""

    val json = parse(rawJson)
    json.extract[List[Project]]
  }

  case class Metric(
                      fieldName: String,
                      fieldType: String,
                      aggregationType: String
                    )

  case class Project(
                       dimensions: List[String],
                       metrics: List[Metric],
                       projectId : String,
                       projectName : String
                     )

  def getMetricColNames(df: DataFrame, filterCols: List[String]): List[String]= {
    val dataFrameColumns = df.columns
    val filtered = dataFrameColumns.filter(x => (!filterCols.exists(_ == x)) &&
      (
        df.schema(x).dataType match {
        case IntegerType | LongType | DoubleType => true case _ => false}
      )
    )
    filtered.toList
  }

  // Get the metric columns with default list of aggregations
  def getAggMetricCols(metricCols: List[Column]): List[Column]= {
    val operations = Seq("min", "max", "avg", "sum", "count")

    // Set aggregations for known high cardinality default event columns
    val highCardinalityCols = List(approx_count_distinct(col("PhoenixIdentity_UserId"), 0.39), approx_count_distinct(col("TargetId"), 0.39))

    val metrics = metricCols.flatMap(c => operations.map(f => aggregationMap(f)(c))) :: highCardinalityCols :: Nil
    metrics.flatMap(m => m)
  }

  def getAggProjectMetricCols(projectMetricCols: List[Metric]): List[Column]= {
    projectMetricCols.map(c =>  aggregationMap(c.aggregationType)(col(s"`Metadata_${c.fieldName}`")))
  }

  def getDimensionCols(df: DataFrame, filterCols: List[String]): List[Column]= {
    df.columns.filter(x => (!filterCols.exists(_ == x))).map(x => (col(x))).toList
  }

  implicit class DataFrameFlattener(df: DataFrame) {
    def flattenSchema: DataFrame = {
      df.select(flatten(Nil, df.schema): _*)
    }

    protected def flatten(path: Seq[String], schema: DataType): Seq[Column] = schema match {
      case s: StructType => {
        s.fields.flatMap(f => flatten(path :+ f.name, f.dataType))
      }
      case other => {
        col(path.map(n => s"`$n`").mkString(".")).as(path.mkString(".")) :: Nil
      }
    }
  }

  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
}
