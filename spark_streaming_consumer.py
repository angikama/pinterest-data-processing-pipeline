import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Download spark sql kafka package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.1 pyspark-shell'

# specify the topic we want to stream data from.
kafka_topic_name = "KafkaPinterest"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .master("local") \
        .appName("KafkaStreaming").getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")


# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load() 

# Select the value part of the kafka message and cast it to a string.
stream_df_2 = stream_df.selectExpr("CAST(value as STRING)")

schema = (
        StructType()
        .add("unique_id", StringType())
        .add("category", StringType())
        .add("title", StringType())
        .add("description", StringType())
        .add("tag_list", StringType())
        .add("is_image_or_video", StringType())
        .add("image_src", StringType())
)

stream_df_3 = stream_df_2.select(from_json(col("value"),schema).alias("stream"))

stream_df_4 = stream_df_3.select("stream.*")

properties = {
        "user":"postgres", "password":"PASSWORD", "driver":"org.postgresql.Driver"
}

def foreach_batch_function(df, epoch_id):
        df.write.jdbc(url="jdbc:postgresql://localhost:5432/pinterest_streaming", table="experimental_data", mode="append", properties=properties)

stream_df_4.writeStream\
        .outputMode("append") \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()
