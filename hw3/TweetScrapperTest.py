from kafka import KafkaProducer
import tweepy
import sys
from json import dumps
from textblob import TextBlob
import json


def getKafkaProducer():
    _producer=None
    try:
        _producer=KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as excep:
        print("Error while connecting to kafka")
        print(str(excep))
    finally:
        return _producer

# def pushToKafka(producer,topic,data):
#     try:
#         key_bytes=bytes()
class StreamListener(tweepy.StreamListener):
    producer=getKafkaProducer()

    class StreamListener(tweepy.StreamListener):
        producer = getKafkaProducer()

        def on_status(self, status):
            if status.lang == 'en':
                if hasattr(status, "extended_tweet"):
                    text = status.extended_tweet["full_text"]
                else:
                    text = status.text
                print(text)
                self.producer.send(str(sys.argv[6]), value=text)


if __name__ == "__main__":
    # complete authorization and initialize API endpoint
    argLen = len(sys.argv)
    if(argLen!=7):
        print()
        print("Usage: <script_name> <consumer key> <consumer secret> <access key> <access secret> <Comma separated tweet keywords> <kafka topic name>")
        print()
        exit(1)

    consumer_key = str(sys.argv[1])
    consumer_secret = str(sys.argv[2])
    access_key = str(sys.argv[3])
    access_secret = str(sys.argv[4])
    tweetTags = str(sys.argv[5])
    filterList = tweetTags.split(',')
    

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # initialize stream
    streamListener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=streamListener,tweet_mode='extended',languages=["en"])
    stream.filter(track=filterList)
