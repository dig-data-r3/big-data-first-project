#!/bin/bash

 hdfs dfs -rm -r output/job3_spark/
"$SPARK_HOME"/bin/spark-submit \
  --master yarn \
  ./job3/spark/spark_job3.py \
  --input_path hdfs:///user/hadoop/input/historical_stock_prices.csv \
  --output_path hdfs:///user/hadoop/output/job3_spark