# big_data_project
## Compilation
1. Set up Kafka.  [Look here](https://kafka.apache.org/quickstart)
- Start Zookeeper on Port 2081 (Standard) 
- Start Kafka on Port 9092 (Standard) 
- Create a Topic "tweet"
2. Collect and analyze Historic Data
- Run `twitter_batch.py` to collect preprocessed Twitter Data with Sentiment Scores as Json File
- Run `spark_offline.py` to analyze the offline data with Spark Structured Streaming
3. Stream and analyze Real Time Data
- Run `twitter_streaming.py`to start the Twitter Stream, which is send to a `KafkaProducer`
- Run `spark_streaming.py` to start analyzing the Stream data with Spark Structured Streaming. 
4. Predict and compare Real Time Data with Historic Data
- Run `twitter_streaming.py` to start the Twitter Stream
- Run `compare_and_predict_online_with_offline.py` to first train LinearRegression Models and calculate an Average of the specific Metrics on the Historic Data and then predict the metrics on the Realtime Data with the Models and compare the metrics of the Realtime Data with the Average of the metrics from the Historic Data.
So the comparison of real time and historic data is shown in the columns: `pos_tweet_count_diff`, `neg_tweet_count_diff` and `neutral_tweet_count_diff`
The Prediction is shown in the columns: `pos_tweet_count_pred`, `neg_tweet_count_pred` and `neutral_tweet_count_pred`

## Used Software/Libraries
- Python 3.7.4
- pyspark 2.4.4
- kafka-python 1.4.6
- tweepy 3.8.0
- tweet-preprocessor 0.5.0
- vaderSentiment 3.2.1
