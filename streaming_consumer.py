from kafka import KafkaConsumer
import json
from json import loads
import boto3

s3_client = boto3.client('s3')

streaming_consumer = KafkaConsumer(
    'pinterest',
    bootstrap_servers = 'localhost:9092',
    value_deserializer = lambda message: loads(message),
    auto_offset_reset = 'earliest'