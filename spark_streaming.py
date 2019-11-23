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
                                                   "avg_sentiment",
                                                   "max_sentiment",
                                                   "min_sentiment",
                                                   "pos_tweet_count_stream",
                                                   "neutral_tweet_count_stream",
                                                   "neg_tweet_count_stream").sort("start", ascending=False)
        #.withColumn("start", date_format(col("start").cast(TimestampType()), "yyyy/MM/dd HH:mm:ss"))\


    if lrModelList is not None:
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
        windowed_stream_df = windowed_stream_df.withColumn("tmp1", lit("mean"))
        windowed_stream_df = windowed_stream_df.alias("stream").join(batch_summary_df.alias("batch"),
                                                     col("stream.tmp1") == col("batch.summary"), "inner")


    query = windowed_stream_df.writeStream\
                            .format("console")\
                            .outputMode("complete")\
                            .option("truncate", False)\
                            .start().awaitTermination()

if __name__ == "__main__":
    analyze_stream_data()



