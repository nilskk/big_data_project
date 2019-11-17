import json
import tweepy
from tweet_preprocessing import Tweet_Preprocessor

CONSUMER_KEY = "fS352dZ2fZ5W09JgJ8zd9HKdU"
CONSUMER_SECRET = "NEcnCZP8LqG20SX53bIle5qPGQB8yMOf7Qj5KHoxMoJGwDJGXL"
ACCESS_TOKEN = "2740635069-c1cCHowhszDsXZoDYN7cPKoKmPStWt9wJ6nqv2r"
ACCESS_TOKEN_SECRET = "CFG7cJc2mkHN9MCeIwXSzcFeQUF2UiZlk9KRxRGtDK9ty"
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


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

            with open("twitter_json/tweets.json", "a") as f:
                f.write("\n")
                json.dump(tweet_dict, f)

    def on_error(self, status_code):
        print(status_code)
        return True


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=auth, listener=myStreamListener, tweet_mode="extended")
myStream.filter(track=["Trump", "Potus", "President of the United States", "Donald Trump", "RealDonaldTrump"], languages=["en"])
