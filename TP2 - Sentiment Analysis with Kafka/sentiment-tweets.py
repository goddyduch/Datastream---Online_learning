"""
Write a script: sentiment-tweets.py that listens to two Kafka topics \"en-tweets\“ and “fr-tweets” and classify the sentiment of the tweets whether the sentiment is positive or negative.
- If the tweet sentiment is positive, write it to the topic \“positive-tweets”.
- If the tweet sentiment is negative, write it to the topic \“negative-tweets”.
"""
from kafka import KafkaConsumer, KafkaProducer
#import river
import json

consumer = KafkaConsumer("en-tweets", "fr-tweets", value_deserializer=lambda m: json.loads(m.decode('utf-16')))
producer = KafkaProducer()

for message in consumer:
    #TODO: implement a model
    print(message)

