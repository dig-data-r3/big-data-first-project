#!/bin/bash

rm -r ./results/job2_spark/
"$SPARK_HOME"/bin/spark-submit \
  --master local[*] \
  ./job2/spark/spark_job2.py \
  --input_hsp file:///home/alex/git/big-data-first-project/dataset/historical_stock_prices$1.csv \
  --input_hs file:///home/alex/git/big-data-first-project/dataset/historical_stocks_clean.csv \
  --output_path file:///home/alex/git/big-data-first-project/results/job2_spark
