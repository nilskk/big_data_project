import preprocessor as p
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class Tweet_Preprocessor(object):

    def __init__(self):
        print(self)

    def clean_tweet(self, tweet):
        p.set_options(p.OPT.URL)
        cleaned_tweet = p.clean(tweet_string=tweet)
        return cleaned_tweet

    def calculate_vader_sentiment(self, tweet):
        sentiment = SentimentIntensityAnalyzer().polarity_scores(tweet)
        return sentiment["compound"]
