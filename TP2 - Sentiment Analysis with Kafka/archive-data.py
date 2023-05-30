"""
Write a script: archive-data.py that archives all topics data (raw-tweets, en-tweets, fr-tweets, positive- tweets, negative-tweets) in a text file.
"""
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer("raw-tweets", "fr-tweets", "en-tweets", "positive-sentiment", "negative-sentiment", value_deserializer=lambda m: json.loads(m.decode('utf-16')))

with open("archive.txt", "a") as f:
    for message in consumer:
        try:
            print(message.value)
            f.write(str(message.value) + "\n")
        except Exception as e:
            print(e)
