#Courtesy of code >>>>> google search


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import json
import time

access_token = "add your own"
access_token_secret =  "add your own"
consumer_key =  "add your own"
consumer_secret =  "add your own"

class StdOutListener(StreamListener):
    
    def on_data(self, data):
        producer.send_messages("pythontest", data.encode('utf-8'))
        d=json.loads(data)
        print(data)
        return True
    
    def on_error(self, status):
        print (status)


kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)

stream.filter(track="nokiahack")
