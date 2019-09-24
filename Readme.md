## Required

### Development Tools (Mac)
#### Scala

Spark requires Scala version 2.11.x
```
brew install scala
```
#### Spark
```
brew install apache-spark
```
#### Foxy Proxy
Install

http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html

### Services

#### Kinesis
Intelligence Events Queue

#### DynamoDB
Stores Kinesis checkpoint.  The checkpoint is updated once a batch of data has been processed successfully.

#### S3
GeoLite-City database storage.  We can potentially store project json configuration here.

#### ElasticSearch
AWS Elasticsearch service used as the main data store for analytics data.

When referencing the ES endpoint in the application code, you should **http** instead of https.

Index mapping should be created beforehand for an Index

e.g.
```
curl -X PUT 'http://***.ap-southeast-1.es.amazonaws.com/phoenixevent-livesg-14106-20171013' -d '{"index": {"number_of_shards": 2,"number_of_replicas": 1 }}'

curl -X PUT 'http://***.ap-southeast-1.es.amazonaws.com/phoenixevent-livesg-14106-20171013/phoenixevent/_mapping' -d  @intelligence-event-mapping.json --header "Content-Type: application/json"
```


#### ES Setup

##### Cluster
- Disable dedicated master
- Enable Zone awareness

##### Network Configuration
- Choose a valid VPC and AZs subnets
- VPC should be set-up with Security Group jetri Private Network Whitelist - To allow only inbound calls within TS internal IP.

##### Access Policy

- Choose 'Do not require signing request with IAM credential'

~~We need to set this so this is only accessible by specific IPs.  The below config does not seem to work.~~

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "es:*",
      "Resource": "arn:/*",
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": [
            "10.0.0.0/16",
            "10.0.0.0/8"
          ]
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::131768523928:role/EMR_EC2_DefaultRole"
      },
      "Action": "es:*",
      "Resource": "arn:/*"
    }
  ]
}
```

## Tools
### IntelliJ Community

IDE used to develop the Spark application.

https://www.jetbrains.com/idea/download/#section=mac

### SBT
Simple Build tool for Scala.

Dependencies are saved in a `build.sbt` file.  IntelliJ can download the dependencies specified in the file automatically.

## Libraries
### Maven
Most dependencies are from maven.  Libraries that you need, you can search and copy the path from Maven directly.

https://mvnrepository.com/

### Maxmind GeoIP

The Maxmind GeoIP2 database from:

```
https://dev.maxmind.com/geoip/geoip2/geolite2/
``` 

To use the database in Scala, the application uses this wrapper:

https://github.com/Sanoma-CDA/maxmind-geoip2-scala

#### Installation

Clone:

https://github.com/Sanoma-CDA/maxmind-geoip2-scala.git

Open terminal on the newly cloned repo, then run:

```
sbt +publish-local
```

This installs maxmind-geoip2-scala as part of your local repository.

Then add this or make sure this is in your SBT file:

```
libraryDependencies += "com.sanoma.cda" %% "maxmind-geoip2-scala" % "1.5.4"
```

## Debugging

### AWS Profile
You have to set the AWS credential in your environment variable.

### ~~Checkpoint~~
~~Use local machine path for checkpoints~~

### IntelliJ Run/Debug Configurations
- Make sure you set-up the application arguments e.g. batch interval, repartition count, Elasticsearch path and checkpoint directory

```
Program Arguments: 10 240 espath /Users/j3.laserna/Documents/SparkStreamingScala/Checkpoint/
```

- Set -Dspark.master=local[*] on VM options

### Notes
Running locally may differ on an actual Hadoop cluster; if you run on IntelliJ it will use Java libraries if it cannot find hadoop libraries.

"withWatermark" does not seem to work on local machines without Hadoop.

## Deployment

### Build

Steps (On terminal):

1) cd to your root project folder
2) type `sbt`
3) type `assembly`; this builds the application
4) copy the build `EventProcessor-assembly-**.jar` from `target/scala-2.11/` directory to a S3 bucket

### EMR

You can run the Spark application either by executing the JAR file via SSH on the Master node or by using the AWS EMR console. 

#### SSH on Master

Preferred approach.

Steps:
1) ssh to the EMR master node
```
ssh -i intelligence-admin.pem  hadoop@xx.xxx.xx.xxx
```
2) copy build from the AWS S3 bucket

```
aws s3 cp s3://intelligence-event-processing-dev/EventProcessor-assembly-1.17.jar ./
```

3) type
- Arguments: batchInterval, repartitionCount, Elasticsearch Path.
- Setting the repartitionCount to zero disregards repartition altogether.
```
spark-submit --deploy-mode cluster --master yarn --executor-memory 20g --conf spark.eventLog.enabled=false --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" EventProcessor-assembly-1.1.14.jar batchInterval repartitionCount
```
and enter

##### Configurations

###### Batch Inteval

This is the batch interval in seconds i.e. if batch interval is 60 seconds, the application will pull 60 seconds worth of data from Kinesis.

###### Repartition Count

This is partitioning the final processed data so that it can be divided according to the resources.  Tuning info below.

###### ElasticSearch Path

Path where to send the data

###### Checkpoint Directory

Spark Checkpoint directory to recover from if exists.

#### AWS EMR console

~~Please take note that you can only [kill](http://docs.aws.amazon.com/solutions/latest/real-time-analytics-spark-streaming/step2.html) the application through the resource manager UI (URL sample below).
Also, the console statuses do not update in a timely manner.  In some cases, the application does not stop even after killing it.~~

1) Copy the Spark application JAR file to Master nodes home folder
2) Select Spark Application on Step Type
3) Select Cluster on Deploy mode
4) Specify Spark Submit options
5) Specify application location
```
file:///home/hadoop/EventProcessor-assembly-1.57.jar
```
6) Specify the project arguments

## Spark Application Monitoring

Steps:
1) create SSH tunnel to master node; use the private IP of the EMR cluster's master node

```
ssh -i intelligence-admin.pem -N -D 8157 hadoop@xx.xxx.x.xxx
```

this allows you to connect the Spark web applications for monitoring.

2) Open in your browser and use the private IP of the EMR cluster's master node to go to the resource manager.

```
http://xx.xxx.x.x:8088/cluster
```

[References](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html)

### Troubleshooting

####Processing Takes a Long time
This may be caused by stopping the application and then running it again after some time.
The application will start processing data from kinesis from where it left off, and depending on the duration from when the application was stopped, this could mean processing ten to hundreds of thousand of records.

##### Solution
Delete dynamo table data.

Running the application creates a table in DynamoDB that handles checkpointing.  The table name will be similar to the application name.

e.g.
```
IntelligenceSparkEventStreamProcessor-emr-dev
```

This is not the ideal situation, but the spark application should not have failed and stopped in the first place.

We may require batch/file processing to populate data that have been missed or to make sure that all data is accounted for.


~~Delete S3 checkpoint folder~~

~~e.g.~~

####Amazon EMR Auto scaling Fails

If setting the autoscaling rules fails; check the error by running describe-cluster on aws cli:

```
aws emr describe-cluster --cluster-id [clusterid]
```

## Spark Tuning

Tuning will take some time.  This will be different based on the data you're trying to process, the volume, what the application is trying to do and type of resource you have.  One thing worth mentioning is Spark is memory intensive.

For Intelligence Events, this is the tuning that worked so far; this will be able to potentially handle additional load as well. i.e. if there are new projects sending new events.

### Amazon EMR


#### 3 r3.2xlarge (5 on Core) 1 r3.xlarge (Master)

Batch Interval: 60 seconds

Repartition Count: 150

With this configuration, the application was able to handle on average 10K records in 50s.

####References

- https://spark.apache.org/docs/latest/configuration.html
- https://aws.amazon.com/blogs/big-data/submitting-user-applications-with-spark-submit/
- http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html
- http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#spark-dynamic-allocation

#####Performance Tuning

- http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-1/
- http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
- https://www.slideshare.net/cloudera/top-5-mistakes-to-avoid-when-writing-apache-spark-applications