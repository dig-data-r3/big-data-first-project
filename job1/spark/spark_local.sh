#!/bin/bash

rm -r ./results/job1_spark/
"$SPARK_HOME"/bin/spark-submit \
  --master local[*] \
  ./job1/spark/spark_job1.py \
  --input_path file:///home/alex/git/big-data-first-project/dataset/historical_stock_prices$1.csv \
  --output_path file:///home/alex/git/big-data-first-project/results/job1_spark
