# Running project

## run servers

### Zookeper
 `/usr/bin/zookeeper-server-start config/zookeeper.properties`
 
### Kafka Server
`/usr/bin/kafka-server-start config/server.properties`

### Run Kafka Producer
`python kafka_server.py`

### Kafka consumer console

`kafka-console-consumer --bootstrap-server localhost:9092 --topic com.udacity.sfcrime.pdcalls --from-beginning`


### Spark Streaming Job

`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py`

# Screen Shots
Screen shots can be found compressed in the `screenshots.zip` file.

# Answers to questions

*How did changing values on the SparkSession property parameters affect the throughput and latency of the data?*

- Some that can be done is to change the number of spark partitions to match the number of kafka partitions. This will make sure we extract as much value as possible from parallelisim.

- Something else we can do is to check in Spark UI that all cores are working.

- Additionally one can increase the driver and workers memory allowing them to operate on larger amounts of data at the same time.

*What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?*

- "spark.executor.memory", "4g"; this allows us to set the mememory the of excecutor. Ideally we would use leave 10% of memory to allow other subprocesses to run. The same can be said about the

- "spark.default.parallelism", 2; this property defines the paralellism to be the same as the one set for Kafka in `server.properties`

- In addtion one can also use a more efficient way to reduce the data serialization overhead; the recommended approach is to use the kyro serializer with the key/value pair property "spark.serializer", "org.apache.spark.serializer.KryoSerializer"


Personaly, I didn't see huge changes when trying different SparkSession configurations; a possibillity for this is that the data being streamed is not large and the system is never under strain. After some reasearch I found that the settings you set for SparkSession / SparkConf aren't the first considerations for throughput or latency unless you are working with stacks such as YARN, possible format conversions (this can become a bottleneck if you are relying on the pipeline to keep up with TensorFlow.

