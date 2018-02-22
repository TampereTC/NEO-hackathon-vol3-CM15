from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import json
import time

access_token = "966056161278615552-wRpIf7lRSHpBVP42p5tpVEY1AVtsPkk"
access_token_secret =  "17Io47qFhEFKDWiUpBtGTKulgsKpWAJ260KIU1z2WOvLj"
consumer_key =  "rltd8Eav7syHQfzKLXisAnvFu"
consumer_secret =  "vVclXwLua2k8RAbmixlniKutG9vX8u7lJxMj5uzllTXtVmHb22"

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
