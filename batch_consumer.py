from kafka import KafkaConsumer
import json
from json import loads
import boto3
import tempfile

s3_client = boto3.client('s3')

batch_consumer = KafkaConsumer(
    'pinterest',
    bootstrap_servers = 'localhost:9092',
    value_deserializer = lambda message: loads(message),
    auto_offset_reset = 'earliest'
)

batch_consumer.subscribe(topics=['KafkaPinterest'])

for msg in batch_consumer:
    print(msg.value)
    file = json.dumps(msg.value)
    id = msg.value['unique_id']
    # Adds object to bucket
    response = s3_client.put_object(
        Body = file,
        Bucket = 'pinterest-data-586afdef-4b18-4000-ba18-b4f49051d72f',
        Key = f'{id}.json'
    )
    