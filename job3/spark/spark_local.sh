#!/bin/bash

# hdfs dfs -rm -r output/job3_spark/

#"$SPARK_HOME"/bin/spark-submit \
#  --master yarn \
#  ./job3/spark/spark_job3.py \
#  --input_path hdfs:///user/alex/input/historical_stock_prices.csv \
#  --output_path hdfs:///user/alex/output/job3_spark

rm -r ./results/job3_spark/
cd "$(dirname "$0")" || exit
"$SPARK_HOME"/bin/spark-submit \
  --master local[4] \
  spark_job3.py \
  --input_path file:///home/alex/git/big-data-first-project/dataset/historical_stock_prices$1.csv \
  --output_path file:///home/alex/git/big-data-first-project/results/job3_spark
