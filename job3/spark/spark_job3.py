#!/usr/bin/env python3
"""spark application"""
import argparse
from datetime import datetime

# create parser and set its arguments
from pyspark.sql import SparkSession


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


def calculate_percent_variation(initial, final):
    return (float(final) - float(initial)) / float(initial) * 100


def compare_months_variations(month_list1, month_list2):
    zip_object = zip(month_list1, month_list2)
    for month1, month2 in zip_object:
        if abs(month1[1] - month2[1]) > THRESHOLD:
            return False
    return True


# fields' index in a row (in historical_stock_prices.csv)
TICKER = 0
OPEN = 1
CLOSE = 2
ADJ_CLOSE = 3
MIN = 4
MAX = 5
VOLUME = 6
DATE = 7

# defines when two tickers are similar based on their monthly percent variation
THRESHOLD = 1

# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Inout file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse parameters
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job3 Spark") \
    .getOrCreate()

sc = spark.sparkContext

# spark.sparkContext.textFile(filepath) returns an RDD
# with a record for each line in the input file
historical_stock_prices = sc.textFile(input_filepath).cache()

# Strip and split by "," records in rdd
split_input = historical_stock_prices.map(lambda line: line.strip().split(','))

# filter the header row and all the years but 2017
# Map into a new RDD -> Key = (Ticker), value = (close_price, date)
ticker_close_date = split_input.filter(lambda x: x[DATE][0:4] == '2017') \
    .map(lambda x: (x[TICKER], float(x[CLOSE]), datetime.strptime(x[DATE], "%Y-%m-%d").date()))

# Map into a new RDD -> Key = (Ticker, month), value = (Ticker, close_price, date)
ticker_month_to_list = ticker_close_date.map(lambda x: ((x[0], x[2].month), x))

# Reduce by key to get, for each ticker and month, the first date and close price of the month
# Map into a new RDD -> Key = (Ticker, month), value = (close price at the first date of the month for that ticker)
ticker_month_to_mindate = ticker_month_to_list.reduceByKey(min_price_and_date) \
    .map(lambda x: (x[0], x[1][1]))

# Reduce by key to get, for each ticker and month, the last date and close price of the month
# Map into a new RDD -> Key = (Ticker, month), value = (close price at the last date of the month for that ticker)
ticker_month_to_maxdate = ticker_month_to_list.reduceByKey(max_price_and_date) \
    .map(lambda x: (x[0], x[1][1]))

# Join ticker_month_to_mindate and ticker_month_to_maxdate on key (Ticker, month)
# Map into a new RDD -> Key = (Ticker, month), value (price percent variation for that ticker in that month)
ticker_month_variation = ticker_month_to_mindate.join(ticker_month_to_maxdate) \
    .map(lambda x: (x[0], calculate_percent_variation(x[1][0], x[1][1])))

# maps to have (ticker: (month, percent_var))
# after groupByKey: (ticker: [(1, 0.15), ... , (12, -3.1)])
# filters out all the tickers that don't have all 12 months (simpler and more efficient like this)
# outputs: (ticker: [list of 12 months with percent_variation])
ticker_aggregate_months = ticker_month_variation.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .groupByKey() \
    .filter(lambda x: len(x[1]) == 12) \
    .map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: y[0]))) \
    .cache()

# Cartesian product to get all possible pairs of ticker
# input: (ticker) : ([month_var_list])
# after cartesian: ((ticker1) : ([month_var_list1]), (ticker2) : ([month_var_list2]))
# Filter to get all pairs of tickers that are similar (percent_var difference per month <= threshold)
ticker_pairs_threshold = ticker_aggregate_months.cartesian(ticker_aggregate_months) \
    .filter(lambda x: x[0][0] < x[1][0] and compare_months_variations(x[0][1], x[1][1]))

# # Map into a new RDD -> Key(Ticker1, Ticker2), value(month, variation1, variation2)
# ticker_pair_to_variations = ticker_pairs_threshold.map(lambda x: ((x[0][0][0], x[1][0][0]), (x[0][0][1], x[0][1], x[1][1])))
#
# # Group by key(Ticker1, Ticker2)
# # Filter when pairs of ticker are similar for all the month of the year
# # Map into a new RDD -> Key = (Ticker1, Ticker2), value = (list of month variations pairs)
# results = ticker_pair_to_variations.groupByKey().filter(lambda x: len(x[1]) == 12)\
#      .map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: y[0]))) \
#      .coalesce(1)

ticker_pairs_threshold.saveAsTextFile(output_filepath)
