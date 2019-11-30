import spark_offline
import spark_streaming

# analyze historic data
batch_df = spark_offline.analyze_offline_data()

# train LinearRegression Models
lrModelList = spark_offline.train_regression_models(batch_df)

# calculate average of metrics
batch_summary_df = spark_offline.get_batch_summary(batch_df)

# compare and predict realtime data with historic data
spark_streaming.analyze_stream_data(lrModelList=lrModelList, batch_summary_df=batch_summary_df)