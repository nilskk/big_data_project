import json
import tweepy
from tweet_preprocessing import Tweet_Preprocessor
from kafka import KafkaProducer
import config as conf

CONSUMER_KEY = conf.consumer_key
CONSUMER_SECRET = conf.consumer_secret
ACCESS_TOKEN = conf.access_token
ACCESS_TOKEN_SECRET = conf.access_token_secret
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# initialize kafka producer on port 9092
producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(MyStreamListener, self).__init__()

    def on_status(self, status):
        if (not status.retweeted) and ('RT @' not in status.text):
            tweet = status.text
            if hasattr(status, "extended_tweet"):
                tweet = status.extended_tweet["full_text"]

            preprocessor = Tweet_Preprocessor()
            clean_tweet = preprocessor.clean_tweet(tweet=tweet)
            sentiment = preprocessor.calculate_vader_sentiment(tweet=clean_tweet)
            print(clean_tweet)

            tweet_dict = {"timestamp": str(status.created_at),
                          "text": clean_tweet,
                          "sentiment": sentiment}

            # send dict with KafkaProducer in topic "tweet"
            producer.send("tweet", value=tweet_dict)

    def on_error(self, status_code):
        print(status_code)
        return True


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=auth, listener=myStreamListener, tweet_mode="extended")
myStream.filter(track=["Trump", "Potus", "President of the United States", "Donald Trump", "RealDonaldTrump"], languages=["en"])
