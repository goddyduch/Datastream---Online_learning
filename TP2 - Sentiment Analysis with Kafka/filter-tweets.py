"""
Write a script: filter-tweets.py that listens to the Kafka topic “raw-tweets” that writes to the topic “en- tweets” if the tweet language is English and writes to “fr-tweets” topic if the tweet language is French.
"""
from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer("raw-tweets", value_deserializer=lambda m: json.loads(m.decode('utf-16')))

en_topic = "en-tweets"
fr_topic = "fr-tweets"

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-16'))

for tweet in consumer:
    lang = tweet.value["lang"]
    if lang == "en":
        producer.send(en_topic, tweet.value)
        print("send to en topic")
    elif lang == "fr":
        producer.send(fr_topic, tweet.value)
        print("send to fr topic")
