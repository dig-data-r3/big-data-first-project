hdfs dfs -rm -r output/job1_mapreduce/
cd "$(dirname "$0")" || exit
mapred streaming \
  -files mapper.py,reducer.py \
  -mapper mapper.py \
  -reducer reducer.py \
  -input input/historical_stock_prices.csv \
  -output output/job1_mapreduce
