"""
Write a script: ingest-tweets.py that stream tweets based on a keyword (along with needed supporting fields like
created_at, lang etc.) from twitter api using tweepy library and ingest them into a kafka topic say “raw-tweets”.
The keyword for streaming tweets can be anything say, Covid, Brexit, China, Russia, Ukraine etc. The data ingested
into the raw-tweets topic can be in json format.
"""
import json
from kafka import KafkaClient, KafkaConsumer, KafkaProducer
import tweepy

bearer = "YOUR_BEARER_KEY"
keyword = "france"
topic = "raw-tweets"

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-16'))

client = tweepy.Client(
    bearer_token=bearer)

# Replace the limit=1000 with the maximum number of Tweets you want
for tweet in tweepy.Paginator(client.search_recent_tweets, query=keyword,
                              tweet_fields=['created_at', 'lang', 'possibly_sensitive'], max_results=100).flatten(
        limit=1000):
    producer.send(topic, tweet.data)
