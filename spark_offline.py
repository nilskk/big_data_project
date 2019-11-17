from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import explode, split, size, from_json, col, window, avg, max, min, count, date_format, when, \
    row_number
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
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
                                             windowed_batch_df.window.end.alias("end"),
                                             "avg_sentiment",
                                             "max_sentiment",
                                             "min_sentiment",
                                             "pos_tweet_count",
                                             "neutral_tweet_count",
                                             "neg_tweet_count")\
                        .withColumn("start", date_format(col("start").cast(TimestampType()), "yyyy/MM/dd HH:mm:ss"))\
                        .withColumn("end", date_format(col("end").cast(TimestampType()), "yyyy/MM/dd HH:mm:ss"))\
                        .sort("start", ascending=True)

# query = windowed_df.writeStream.format("es")\
#                                 .option("es.nodes", "localhost")\
#                                 .option("es.port", "9200")\
#                                 .option("es.resource", "es_spark_tweet")\
#                                 .option("checkpointLocation", "es_spark_checkpoint/tweet")\
#                                 .outputMode("append")\
#                                 .start().awaitTermination()

batch_query = windowed_batch_df.show(50)