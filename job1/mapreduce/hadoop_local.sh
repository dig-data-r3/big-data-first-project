hdfs dfs -rm -r output/job1_mapreduce/

mapred streaming \
  -mapper /home/alex/git/big-data-first-project/job1/mapreduce/mapper.py \
  -reducer /home/alex/git/big-data-first-project/job1/mapreduce/reducer.py \
  -input input/historical_stock_prices.csv \
  -output output/job1_mapreduce
