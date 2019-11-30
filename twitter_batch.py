import json
import tweepy
from tweet_preprocessing import Tweet_Preprocessor
import config as conf

# set key and secrets from config.py
CONSUMER_KEY = conf.consumer_key
CONSUMER_SECRET = conf.consumer_secret
ACCESS_TOKEN = conf.access_token
ACCESS_TOKEN_SECRET = conf.access_token_secret
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# listen to tweepy stream
class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(MyStreamListener, self).__init__()

    def on_status(self, status):
        # remove retweets
        if (not status.retweeted) and ('RT @' not in status.text):
            tweet = status.text
            # look for extended tweets and get text
            if hasattr(status, "extended_tweet"):
                tweet = status.extended_tweet["full_text"]

            preprocessor = Tweet_Preprocessor()
            # clean tweet
            clean_tweet = preprocessor.clean_tweet(tweet=tweet)
            # calculate sentiment
            sentiment = preprocessor.calculate_vader_sentiment(tweet=clean_tweet)
            print(clean_tweet)

            # create dict with timestamp, tweettext and sentiment of the text
            tweet_dict = {"timestamp": str(status.created_at),
                          "text": clean_tweet,
                          "sentiment": sentiment}

            # write to json file
            with open("twitter_json/tweets.json", "a") as f:
                f.write("\n")
                json.dump(tweet_dict, f)

    def on_error(self, status_code):
        print(status_code)
        return True


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=auth, listener=myStreamListener, tweet_mode="extended")
# filter tweepy stream for tags about Trump and only english language tweets
myStream.filter(track=["Trump", "Potus", "President of the United States", "Donald Trump", "RealDonaldTrump"], languages=["en"])
