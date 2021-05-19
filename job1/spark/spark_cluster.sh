#!/bin/bash

 hdfs dfs -rm -r output/job1_spark/
"$SPARK_HOME"/bin/spark-submit \
  --master yarn \
  ./job1/spark/spark_job1.py \
  --input_path hdfs:///input/historical_stock_prices.csv \
  --output_path hdfs:///output/job1_spark