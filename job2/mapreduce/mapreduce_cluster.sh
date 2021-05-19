#!/bin/bash

hdfs dfs -rm -r output/job2_mapreduce/
cd "$(dirname "$0")" || exit
  # -D mapreduce.job.reduces=2 \
  #- D stream.num.map.output.key.fields=3 \
  # -D mapreduce.partition.keypartitioner.options=-k1,1 \
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -D mapreduce.job.reduces=1 \
  -files mapper.py,reducer.py,../../dataset/historical_stocks_clean.csv \
  -mapper mapper.py \
  -reducer reducer.py \
  -input input/historical_stock_prices$1.csv \
  -output output/job2_mapreduce
