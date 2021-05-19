#!/bin/bash

 hdfs dfs -rm -r output/job2_spark/
"$SPARK_HOME"/bin/spark-submit \
  --master yarn \
  ./job2/spark/spark_job2.py \
  --input_path hdfs:///input/historical_stock_prices.csv \
  --output_path hdfs:///output/job2_spark