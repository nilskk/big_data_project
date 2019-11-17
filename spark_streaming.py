from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import explode, split, size, from_json, col, window, avg, min, max, count, date_format, when
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, IntegerType, NumericType
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 ' \
                                    '--jars elasticsearch-hadoop-7.4.0/dist/elasticsearch-spark-20_2.11-7.4.0.jar ' \
                                    'pyspark-shell'

spark = SparkSession.builder.appName("TwitterSpark").getOrCreate()

schema = StructType([StructField("timestamp", TimestampType(), True),
                     StructField("text", StringType(), True),
                     StructField("sentiment", DoubleType(), True)])

batch_df = spark\
        .read\
        .schema(schema)\
        .format("json")\
        .load(os.path.realpath("twitter_json"))

windowed_batch_df = batch_df.withWatermark("timestamp", "10 seconds")\
                    .groupBy(window("timestamp", "30 seconds"))\
                    .agg(avg("sentiment").alias("avg_sentiment"),
                         max("sentiment").alias("max_sentiment"),
                         min("sentiment").alias("min_sentiment"),
                         count(when(batch_df["sentiment"]>0.2, True)).alias("pos_tweet_count"),
                         count(when((batch_df["sentiment"]<=0.2) & (batch_df["sentiment"]>=-0.2), True)).alias("neutral_tweet_count"),
                         count(when(batch_df["sentiment"]<-0.2, True)).alias("neg_tweet_count"))

windowed_batch_df = windowed_batch_df.select(windowed_batch_df.window.start.alias("start"),
                                             "avg_sentiment",
                                             "max_sentiment",
                                             "min_sentiment",
                                             "pos_tweet_count",
                                             "neutral_tweet_count",
                                             "neg_tweet_count")\
                        .withColumn("start_int", col("start").cast(IntegerType())) \
                        .withColumn("start", date_format(col("start").cast(TimestampType()), "yyyy/MM/dd HH:mm:ss"))\
                        .sort("start", ascending=True)\

featAssembler = VectorAssembler(inputCols=["avg_sentiment", "max_sentiment", "min_sentiment"], outputCol="features")
windowed_batch_df = featAssembler.transform(windowed_batch_df)

lr1 = LinearRegression(labelCol="pos_tweet_count", predictionCol="pos_tweet_count_pred", maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel1 = lr1.fit(windowed_batch_df)

lr2 = LinearRegression(labelCol="neutral_tweet_count", predictionCol="neutral_tweet_count_pred", maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel2 = lr2.fit(windowed_batch_df)

lr3 = LinearRegression(labelCol="neg_tweet_count", predictionCol="neg_tweet_count_pred", maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel3 = lr3.fit(windowed_batch_df)

windowed_batch_df.show(50)


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
         count(when(stream_df["sentiment"] > 0.2, True)).alias("pos_tweet_count"),
         count(when((stream_df["sentiment"] <= 0.2) & (stream_df["sentiment"] >= -0.2), True)).alias(
             "neutral_tweet_count"),
         count(when(stream_df["sentiment"] < -0.2, True)).alias("neg_tweet_count"))

windowed_stream_df = windowed_stream_df.select(windowed_stream_df.window.start.alias("start"),
                                               "avg_sentiment",
                                               "max_sentiment",
                                               "min_sentiment",
                                               "pos_tweet_count",
                                               "neutral_tweet_count",
                                               "neg_tweet_count") \
    .withColumn("start_int", col("start").cast(IntegerType())) \
    .withColumn("start", date_format(col("start").cast(TimestampType()), "yyyy/MM/dd HH:mm:ss"))\
    .sort("start", ascending=True)

featAssembler = VectorAssembler(inputCols=["avg_sentiment", "max_sentiment", "min_sentiment"], outputCol="features")
windowed_stream_df = featAssembler.transform(windowed_stream_df)

windowed_stream_df = lrModel1.transform(windowed_stream_df)
windowed_stream_df = lrModel2.transform(windowed_stream_df)
windowed_stream_df = lrModel3.transform(windowed_stream_df)

windowed_stream_df = windowed_stream_df.withColumn("pos_tweet_error", col("pos_tweet_count") - col("pos_tweet_count_pred"))\
                    .withColumn("neutral_tweet_error", col("neutral_tweet_count") - col("neutral_tweet_count_pred"))\
                    .withColumn("neg_tweet_error", col("neg_tweet_count") - col("neg_tweet_count_pred"))\
                    .select("start", "avg_sentiment", "max_sentiment", "min_sentiment",
                            "pos_tweet_count", "pos_tweet_count_pred", "pos_tweet_error",
                            "neutral_tweet_count", "neutral_tweet_count_pred", "neutral_tweet_error",
                            "neg_tweet_count", "neg_tweet_count_pred", "neg_tweet_error")


query = windowed_stream_df.writeStream\
                            .format("console")\
                            .outputMode("complete")\
                            .start().awaitTermination()

# query = windowed_df.writeStream.format("es")\
#                                 .option("es.nodes", "localhost")\
#                                 .option("es.port", "9200")\
#                                 .option("es.resource", "es_spark_tweet")\
#                                 .option("checkpointLocation", "es_spark_checkpoint/tweet")\
#                                 .outputMode("append")\
#                                 .start().awaitTermination()


