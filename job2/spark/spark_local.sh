#!/bin/bash

# hdfs dfs -rm -r output/job2_spark/

#"$SPARK_HOME"/bin/spark-submit \
#  --master yarn \
#  ./job2/spark/spark_job2.py \
#  --input_path hdfs:///user/alex/input/historical_stock_prices.csv \
#  --output_path hdfs:///user/alex/output/job2_spark

rm -r ./results/job2_spark/

"$SPARK_HOME"/bin/spark-submit \
  --master local[4] \
  ./job2/spark/spark_job2.py \
  --input_path file:///home/alex/git/big-data-first-project/dataset/historical_stock_prices.csv \
  --output_path file:///home/alex/git/big-data-first-project/results/job2_spark
