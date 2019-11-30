from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import explode, split, size, from_json, col, window, avg, min, max, count, date_format, when
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, IntegerType, NumericType
import os


def analyze_offline_data():
    # add kafka dependency to pyspark
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 ' \
                                        'pyspark-shell'

    # create SparkSession
    spark = SparkSession.builder.appName("TwitterSparkBatch").getOrCreate()

    # define schema exactly like the dict in twitter_batch.py
    schema = StructType([StructField("timestamp", TimestampType(), True),
                         StructField("text", StringType(), True),
                         StructField("sentiment", DoubleType(), True)])

    # read json files from directory "twitter_json" and parse to dataframe with the defined schema
    batch_df = spark\
            .read\
            .schema(schema)\
            .format("json")\
            .load(os.path.realpath("twitter_json"))

    # set window of 30 seconds with watermark of 10 seconds and aggregate min, avg, and max sentiment over the windows,
    # as well as the amount of tweets with positive, negative and neutral sentiment in that window
    windowed_batch_df = batch_df.withWatermark("timestamp", "10 seconds")\
                        .groupBy(window("timestamp", "30 seconds"))\
                        .agg(avg("sentiment").alias("avg_sentiment"),
                             max("sentiment").alias("max_sentiment"),
                             min("sentiment").alias("min_sentiment"),
                             count(when(batch_df["sentiment"]>0.1, True)).alias("pos_tweet_count_batch"),
                             count(when((batch_df["sentiment"]<=0.1) & (batch_df["sentiment"]>=-0.1), True)).alias("neutral_tweet_count_batch"),
                             count(when(batch_df["sentiment"]<-0.1, True)).alias("neg_tweet_count_batch"))

    windowed_batch_df = windowed_batch_df.select(windowed_batch_df.window.start.alias("start"),
                                                 "min_sentiment",
                                                 "avg_sentiment",
                                                 "max_sentiment",
                                                 "pos_tweet_count_batch",
                                                 "neutral_tweet_count_batch",
                                                 "neg_tweet_count_batch").sort("start", ascending=False)

    # show 50 newest results if there are more than 50
    windowed_batch_df.show(50)

    return windowed_batch_df


def train_regression_models(dataframe: DataFrame):
    # create Feature vector with avg, min, max sentiment for regression
    featAssembler = VectorAssembler(inputCols=["avg_sentiment", "max_sentiment", "min_sentiment"], outputCol="features")
    dataframe = featAssembler.transform(dataframe)

    # train LinearRegression Model to predict the amount of positive tweets in a window
    lr1 = LinearRegression(labelCol="pos_tweet_count_batch", predictionCol="pos_tweet_count_pred", maxIter=10, regParam=0.3,
                           elasticNetParam=0.8)
    lrModel1 = lr1.fit(dataframe)

    # train LinearRegression Model to predict the amount of neutral tweets in a window
    lr2 = LinearRegression(labelCol="neutral_tweet_count_batch", predictionCol="neutral_tweet_count_pred", maxIter=10,
                           regParam=0.3, elasticNetParam=0.8)
    lrModel2 = lr2.fit(dataframe)

    # train LinearRegression Model to predict the amount of negative tweets in a window
    lr3 = LinearRegression(labelCol="neg_tweet_count_batch", predictionCol="neg_tweet_count_pred", maxIter=10, regParam=0.3,
                           elasticNetParam=0.8)
    lrModel3 = lr3.fit(dataframe)

    return [lrModel1, lrModel2, lrModel3]


def get_batch_summary(dataframe: DataFrame):
    # get summary of dataframe for amount of positive, negative and neutral tweets, to extract average of these values
    # for comparison with the values from the online data
    summary_batch_df = dataframe.describe(["pos_tweet_count_batch", "neutral_tweet_count_batch", "neg_tweet_count_batch"])
    summary_batch_df.show()
    return summary_batch_df

if __name__ == "__main__":
    analyze_offline_data()


