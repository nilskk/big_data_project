import spark_offline
import spark_streaming

batch_df = spark_offline.analyze_offline_data()
lrModelList = spark_offline.train_regression_models(batch_df)
batch_summary_df = spark_offline.get_batch_summary(batch_df)

spark_streaming.analyze_stream_data(lrModelList=lrModelList, batch_summary_df=batch_summary_df)