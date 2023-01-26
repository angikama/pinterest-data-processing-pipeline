# pinterest-data-processing-pipeline
# Pinterest Data Processing Pipeline

## Purpose
- Replicating the Pinterest experimentation pipeline
- The pipeline takes advantage of Lambda architecture which uses batch and streaming processing

## Tools Used
- Python (Pyspark)
- Apache Spark, Airflow and Kafka
- PostgresSQL

### Data Ingestion
- The Kafka Producer within the project API sends data to the Kafka topic
- The data sent mimic pinterest posts with information like the category, title and description of the pin
- For batch processing, a consumer is built to recieve the data from the topic

### Batch Processing
- To store the batch data, an Amazon S3 bucket was used
- Using boto3 messages are extracted from the consumer and saved to the S3 bucket as JSON files with the post's unique id as the file name
- To process this batch data, the JSON files are imported into Apache Spark
- With Spark, data cleaning and transformation can be performed (e.g Changing column names, removing duplicates, removing empty rows)

### Orchestrating Batch Processing with Airflow
- Apache Airflow is used to manage the batch processing pipeling
- The DAG written allows Airflow to fulfill the spark script daily
  - Pinterest historically used Pinball for this, however it has since been depreciated

### Streaming & Storage
- To send streaming data to Apache Spark, Kafka and Spark were connected
- Data consumed is saved to a PostgresSQL database
