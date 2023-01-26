from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark import SparkContext, SparkConf
import os

# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId="access_key"
secretAccessKey="secret_access_key"
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

# Read from the S3 bucket
df = spark.read.json("s3a://pinterest-data-586afdef-4b18-4000-ba18-b4f49051d72f/*.json")
# Dropping columns that are not needed
df_2 = df.drop("downloaded", "save_location", "index")
# Neatens up the tag list if the post does not have any
df_3 = df_2.withColumn("tag_list", when(df.tag_list == "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "Null").otherwise(df.tag_list))
df_3.show()


