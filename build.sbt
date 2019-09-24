name := "EventProcessor"

version := "1.1.31"

organization := "com.jetri.intelligence"

scalaVersion := "2.11.8"

// group - library - spark version
// "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.1.1"
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
"org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
"org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.2.0",
"com.amazonaws" % "amazon-kinesis-client" % "1.8.1",
"org.elasticsearch" %% "elasticsearch-spark-20" % "5.6.0",
"com.sanoma.cda" %% "maxmind-geoip2-scala" % "1.5.4",
"org.apache.hadoop" % "hadoop-aws" % "2.8.1"
)

// Reference
// https://stackoverflow.com/questions/17461453/build-scala-and-symbols-meaning
// https://stackoverflow.com/questions/30851244/spark-read-file-from-s3-using-sc-textfile-s3n
// https://stackoverflow.com/questions/25144484/sbt-assembly-deduplication-found-error
// https://github.com/databricks/learning-spark/blob/master/build.sbt

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}