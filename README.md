# big_data_project
## Compilation
1. Set up Kafka.  [Look here](https://kafka.apache.org/quickstart)
- Start Zookeeper on Port 2081 (Standard) 
- Start Kafka on Port 9092 (Standard) 
- Create a Topic "tweet"
2. Collect Historic Data
- Run `twitter_batch.py` to collect preprocessed Twitter Data with Sentiment Scores
3. Use Spark for Analysis
- Run `twitter_streaming.py`to start the Twitter Stream, which is send to a `KafkaProducer`
- Run `spark_streaming.py` to start Spark. It first analyzes the historic data 
and trains a `LinearRegression` Model on the metrics. 
Than the Real-Time Data, which is send from the `KafkaProducer` is analyzed 
and predictions for metrics are made, based on the Regression Model.
The results are printed on the console.

## Used Software/Libraries
- Python 3.7.4
- pyspark 2.4.4
- kafka-python 1.4.6
- tweepy 3.8.0
- tweet-preprocessor 0.5.0
- vaderSentiment 3.2.1
