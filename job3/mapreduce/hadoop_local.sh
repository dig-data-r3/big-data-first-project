#!/bin/bash

hdfs dfs -rm -r output/job3_mapreduce/
#hdfs dfs -rm -r tmp/job3_mapreduce
cd "$(dirname "$0")" || exit
#  -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
#  -D stream.num.map.output.key.fields=2 \
#  -D mapreduce.partition.keycomparator.options=-k1,2n \
#mapred streaming \
#  -files firstMapper.py,firstReducer.py \
#  -mapper firstMapper.py \
#  -reducer firstReducer.py \
#  -input input/historical_stock_prices.csv \
#  -output tmp/job3_mapreduce \
#&& \
mapred streaming \
  -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
  -D stream.num.map.output.key.fields=2 \
  -D mapreduce.partition.keycomparator.options=-k2,2n \
  -files secondMapper.py,secondReducer.py \
  -mapper secondMapper.py \
  -reducer secondReducer.py \
  -input tmp/job3_mapreduce/part-* \
  -output output/job3_mapreduce
