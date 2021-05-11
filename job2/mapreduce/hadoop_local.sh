#!/bin/bash

hdfs dfs -rm -r output/job2_mapreduce/
cd "$(dirname "$0")" || exit
  # -D mapreduce.job.reduces=2 \
  #- D stream.num.map.output.key.fields=3 \
  # -D mapreduce.partition.keypartitioner.options=-k1,1 \
mapred streaming \
  -files mapper.py,reducer.py,../../dataset/historical_stocks_clean.csv \
  -mapper mapper.py \
  -reducer reducer.py \
  -input input/historical_stock_prices.csv \
  -output output/job2_mapreduce