#!/bin/bash

 hdfs dfs -rm -r output/job2_spark/
"$SPARK_HOME"/bin/spark-submit \
  --master yarn \
  ./job2/spark/spark_job2.py \
  --input_hsp hdfs:///user/hadoop/input/historical_stock_prices$1.csv \
  --input_hs hdfs:///user/hadoop/input/historical_stocks_clean.csv \
  --output_path hdfs:///user/hadoop/output/job2_spark