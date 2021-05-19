#!/bin/bash

hdfs dfs -rm -r output/job1_mapreduce/
cd "$(dirname "$0")" || exit
mapred streaming \
  -D mapreduce.job.reduces=1 \
  -files mapper.py,reducer.py \
  -mapper mapper.py \
  -reducer reducer.py \
  -input input/historical_stock_prices$1.csv \
  -output output/job1_mapreduce
