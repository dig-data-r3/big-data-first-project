hdfs dfs -rm -r output/job2_mapreduce/
cd "$(dirname "$0")" || exit
mapred streaming \
  # -D mapreduce.job.reduces=2 \
  #- D stream.num.map.output.key.fields=3 \
  # -D mapreduce.partition.keypartitioner.options=-k1,1 \
  # makes the files available locally on the compute nodes
  # loads the dataset into the distributed cache available on all computate nodes
  -files mapper.py,reducer.py,../../dataset/historical_stocks_clean.csv \
  -mapper mapper.py \
  -reducer reducer.py \
  -input input/historical_stock_prices_tiny.csv \
  -output output/job2_mapreduce
