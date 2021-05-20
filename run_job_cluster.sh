#!/bin/bash

# ./run_job_local n_job application dataset_size
# n_job = 1, 2, 3
# application = hadoop, hive, spark
# dataset_size = 256, 512, 1024, default=2048MB (empty filename), 4096

if [ $# -eq 0 ];
then
	echo "Error: Too few arguments."
    echo "Usage: ./run_job_local n_job application [dataset_size (MB)]"
    echo "n_job = 1, 2, 3"
    echo "application = mapreduce, hive, spark"
    echo "dataset_size = 256, 512, 1024, default=2048MB (empty filename), 4096"
    exit 1
fi


if [ $2 == 'hive' ];
then
	time hive -hivevar DATASET_NAMESIZE=historical_stock_prices$3 --f ./job$1/$2/job$1.hql
else
	time ./job$1/$2/$2_cluster.sh $3
fi

# notify-send "Job $1 $2 finished!"
