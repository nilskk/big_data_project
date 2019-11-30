from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import from_json, col, window, avg, min, max, count, date_format, when, lit
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, IntegerType, NumericType
import os

def analyze_stream_data(lrModelList = None, batch_summary_df: DataFrame = None):

    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 ' \
                                        'pyspark-shell'
    spark = SparkSession.builder.appName("TwitterSparkStream").getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    schema = StructType([StructField("timestamp", TimestampType(), True),
                         StructField("text", StringType(), True),
                         StructField("sentiment", DoubleType(), True)])
    # initialize Spark Structured Stream with KafkaConsumer subscribing to topic "tweet" from twitter_stream.py
    # parse the kafka stream to the schema
    stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweet") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select(col("data.timestamp").alias("timestamp"), col("data.text").alias("text"),
                col("data.sentiment").alias("sentiment"))

    # set window of 30 seconds with watermark of 10 seconds and aggregate min, avg, and max sentiment over the windows,
    # as well as the amount of tweets with positive, negative and neutral sentiment in that window
    windowed_stream_df = stream_df.withWatermark("timestamp", "10 seconds") \
        .groupBy(window("timestamp", "30 seconds")) \
        .agg(avg("sentiment").alias("avg_sentiment"),
             max("sentiment").alias("max_sentiment"),
             min("sentiment").alias("min_sentiment"),
             count(when(stream_df["sentiment"] > 0.1, True)).alias("pos_tweet_count_stream"),
             count(when((stream_df["sentiment"] <= 0.1) & (stream_df["sentiment"] >= -0.1), True)).alias(
                 "neutral_tweet_count_stream"),
             count(when(stream_df["sentiment"] < -0.1, True)).alias("neg_tweet_count_stream"))

    windowed_stream_df = windowed_stream_df.select(windowed_stream_df.window.start.alias("start"),
                                                   "min_sentiment",
                                                   "avg_sentiment",
                                                   "max_sentiment",
                                                   "pos_tweet_count_stream",
                                                   "neutral_tweet_count_stream",
                                                   "neg_tweet_count_stream").sort("start", ascending=False)


    if lrModelList is not None:
        # if there are LinearRegression Models as function arguments, predict the amount of positive, negative and
        # neutral tweets, based on the models trained on the offline data in spark_offline.py
        featAssembler = VectorAssembler(inputCols=["avg_sentiment", "max_sentiment", "min_sentiment"], outputCol="features")
        windowed_stream_df = featAssembler.transform(windowed_stream_df)

        windowed_stream_df = lrModelList[0].transform(windowed_stream_df)
        windowed_stream_df = lrModelList[1].transform(windowed_stream_df)
        windowed_stream_df = lrModelList[2].transform(windowed_stream_df)

        windowed_stream_df = windowed_stream_df.select("start", "avg_sentiment", "max_sentiment", "min_sentiment",
                    "pos_tweet_count_stream", "pos_tweet_count_pred",
                    "neutral_tweet_count_stream", "neutral_tweet_count_pred",
                    "neg_tweet_count_stream", "neg_tweet_count_pred")

    if batch_summary_df is not None:
        # if there is a dataframe as function argument with the average values of the specific metrics from offline data
        # then calculate the difference between the amount of positive, negative and neutral tweets in the online data
        # with the average of the specific metric from the offline data for a comparison
        windowed_stream_df = windowed_stream_df.withColumn("tmp1", lit("mean"))
        windowed_stream_df = windowed_stream_df.alias("stream").join(batch_summary_df.alias("batch"),
                                                     col("stream.tmp1") == col("batch.summary"), "inner")

        windowed_stream_df = windowed_stream_df\
                                .withColumn("pos_tweet_count_diff", col("pos_tweet_count_stream") - col("pos_tweet_count_batch"))\
                                .withColumn("neutral_tweet_count_diff", col("neutral_tweet_count_stream") - col("neutral_tweet_count_batch")) \
                                .withColumn("neg_tweet_count_diff", col("neg_tweet_count_stream") - col("neg_tweet_count_batch"))

        windowed_stream_df = windowed_stream_df.select("start", "min_sentiment", "avg_sentiment", "max_sentiment",
                                                       "pos_tweet_count_batch", "pos_tweet_count_stream", "pos_tweet_count_diff", "pos_tweet_count_pred",
                                                       "neutral_tweet_count_batch", "neutral_tweet_count_stream", "neutral_tweet_count_diff", "neutral_tweet_count_pred",
                                                       "neg_tweet_count_batch", "neg_tweet_count_stream", "neg_tweet_count_diff", "neg_tweet_count_pred")


    # write spark stream to console in output mode "complete"
    query = windowed_stream_df.writeStream\
                            .format("console")\
                            .outputMode("complete")\
                            .option("truncate", False)\
                            .start().awaitTermination()

if __name__ == "__main__":
    analyze_stream_data()



