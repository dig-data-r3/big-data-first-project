#!/bin/bash

# hdfs dfs -rm -r output/job2_spark/

#"$SPARK_HOME"/bin/spark-submit \
#  --master yarn \
#  ./job2/spark/spark_job2.py \
#  --input_path hdfs:///user/alex/input/historical_stock_prices.csv \
#  --output_path hdfs:///user/alex/output/job2_spark

rm -r ./results/job2_spark/
cd "$(dirname "$0")" || exit
"$SPARK_HOME"/bin/spark-submit \
  --master local[4] \
  spark_job2.py \
  --input_hsp ../../dataset/historical_stock_prices$1.csv \
  --input_hs ../../dataset/historical_stocks_clean.csv \
  --output_path ../../results/job2_spark
