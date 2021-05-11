$SPARK_HOME/bin/spark-submit \
  --master yarn \
  ./job1/spark/spark_job1.py \
  --input_path hdfs:///user/alex/input/historical_stock_prices.csv \
  --output_path hdfs:///user/alex/output/job1_spark
