#!/bin/bash

# hdfs dfs -rm -r output/job1_spark/

#"$SPARK_HOME"/bin/spark-submit \
#  --master yarn \
#  ./job1/spark/spark_job1.py \
#  --input_path hdfs:///user/alex/input/historical_stock_prices.csv \
#  --output_path hdfs:///user/alex/output/job1_spark

rm -r ./results/job1_spark/
cd "$(dirname "$0")" || exit
"$SPARK_HOME"/bin/spark-submit \
  --master local[4] \
  spark_job1.py \
  --input_path file:///home/alex/git/big-data-first-project/dataset/historical_stock_prices$1.csv \
  --output_path file:///home/alex/git/big-data-first-project/results/job1_spark
