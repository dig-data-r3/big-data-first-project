#!/usr/bin/env python3

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark
sc = SparkContext (
   "local",
   "storagelevel app"
)


def min_price_and_date(x, y):
    x_price = x[1]
    y_price = y[1]
    x_date = x[2]
    y_date = y[2]

    if x_date <= y_date:
        return x[0], x_price, x_date
    else:
        return y[0], y_price, y_date

def max_price_and_date(x, y):
    x_price = x[1]
    y_price = y[1]
    x_date = x[2]
    y_date = y[2]

    if x_date >= y_date:
        return x[0], x_price, x_date
    else:
        return y[0], y_price, y_date


# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Inout file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse parameters
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# inizialize SparkSession
spark = SparkSession.builder.appName("Job3 Spark").getOrCreate()

sc = spark.sparkContext

historical_stock_prices = sc.textFile(input_filepath).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)

# Strip and split by "," records in rdd
split_input = historical_stock_prices.map(lambda line: line.strip().split(","))

# Remove first row from rdd
remove_first_row = split_input.filter(lambda line: line[0] != 'ticker')

# Map into a new RDD -> Key = (Ticker), value = (close_price, date)
split_input_datetime = remove_first_row.map(lambda x: (x[0], float(x[2]), datetime.strptime(x[7], "%Y-%m-%d").date()))

# Filter only 2017 data
data_2017 = split_input_datetime.filter(lambda x: x[2].year == 2017)

# Map into a new RDD -> Key = (Ticker, month), value = (Ticker, close_price, date)
ticker_month_to_list = data_2017.map(lambda x: ((x[0], x[2].month), x))

# Reduce by key to get, for each ticker and month, the first date and close price of the month
# Map into a new RDD -> Key = (Ticker, month), value = (close price at the first date of the month for that ticker)
ticker_month_to_mindate = ticker_month_to_list.reduceByKey(min_price_and_date).map(lambda x: (x[0], x[1][1]))

# Reduce by key to get, for each ticker and month, the last date and close price of the month
# Map into a new RDD -> Key = (Ticker, month), value = (close price at the last date of the month for that ticker)
ticker_month_to_maxdate = ticker_month_to_list.reduceByKey(max_price_and_date).map(lambda x: (x[0], x[1][1]))

# Join ticker_month_to_mindate and ticker_month_to_maxdate on key (Ticker, month)
# Map into a new RDD -> Key = (Ticker, month), value (price variation for that ticker in that month)
ticker_month_variation = ticker_month_to_mindate.join(ticker_month_to_maxdate).map(lambda x: (x[0], ((x[1][1]-x[1][0])/x[1][0])*100))

# Cartesian product to get all possible pairs of ticker
# Filter to get all pairs of ticker that are similar
ticker_pairs_trashold = ticker_month_variation.cartesian(ticker_month_variation).filter(lambda x: abs(x[0][1] - x[1][1]) <= 1 and x[0][0][1] == x[1][0][1] and x[0][0][0] < x[1][0][0])

# Map into a new RDD -> Key(Ticker1, Ticker2), value(month, variation1, variation2)
ticker_pair_to_variations = ticker_pairs_trashold.map(lambda x: ((x[0][0][0], x[1][0][0]), (x[0][0][1], x[0][1], x[1][1])))

# Group by key(Ticker1, Ticker2)
# Filter when pairs of ticker are similar for all the month of the year
# Map into a new RDD -> Key = (Ticker1, Ticker2), value = (list of month variations pairs)
results = ticker_pair_to_variations.groupByKey().filter(lambda x: len(x[1]) == 12)\
     .map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: y[0]))) \
     .sortBy(keyfunc=lambda x: x[0], ascending=False) \
     .coalesce(1)

results.saveAsTextFile(output_filepath)

