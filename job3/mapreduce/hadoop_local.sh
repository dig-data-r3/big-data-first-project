#!/bin/bash

hdfs dfs -rm -r output/job3_mapreduce/
#hdfs dfs -rm -r tmp/
cd "$(dirname "$0")" || exit
  # -D mapreduce.job.reduces=2 \
  #- D stream.num.map.output.key.fields=3 \
  # -D mapreduce.partition.keypartitioner.options=-k1,1 \
mapred streaming \
  -D mapreduce.job.reduces=1 \
  -files firstMapper.py,firstReducer.py \
  -mapper firstMapper.py \
  -reducer firstReducer.py \
  -input input/historical_stock_prices.csv \
  -output tmp/job3_mapreduce \
#&& \
#mapred streaming \
#  -files secondMapper.py,secondReducer.py \
#  -mapper secondMapper.py \
#  -reducer secondReducer.py \
#  -input tmp/part-* \
#  -output output/job3_mapreduce
