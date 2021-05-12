#!/usr/bin/env python3
"""spark application"""
import argparse
from datetime import datetime

# create parser and set its arguments
from pyspark.sql import SparkSession


def min_date(a, b):
    date_a = datetime.strptime(a[1], '%Y-%m-%d').date()
    date_b = datetime.strptime(b[1], '%Y-%m-%d').date()
    if date_a <= date_b:
        return a
    else:
        return b


def max_date(a, b):
    date_a = datetime.strptime(a[1], '%Y-%m-%d').date()
    date_b = datetime.strptime(b[1], '%Y-%m-%d').date()
    if date_a >= date_b:
        return a
    else:
        return b


def calculate_percent_variation(initial, final):
    return (float(final) - float(initial)) / float(initial) * 100


# fields index in a row (in historical_stock_prices.csv)
TICKER = 0
OPEN = 1
CLOSE = 2
ADJ_CLOSE = 3
MIN = 4
MAX = 5
VOLUME = 6
DATE = 7

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job1 Spark") \
    .getOrCreate()

sc = spark.sparkContext

# spark.sparkContext.textFile(filepath) returns an RDD
# with a record for each line in the input file
historical_stock_prices = sc.textFile(input_filepath).cache()

split_input = historical_stock_prices.map(lambda line: line.strip().split(','))
# filter the header row
remove_first_row = split_input.filter(lambda line: line[TICKER] != 'ticker')

# new RDD with a key-value pair of ticker and (close, date). We need to save the close price together with the date
ticker_date = remove_first_row.map(lambda line: (line[TICKER], (line[CLOSE], line[DATE])))
first_quot_date = ticker_date.reduceByKey(lambda a, b: min_date(a, b))
last_quot_date = ticker_date.reduceByKey(lambda a, b: max_date(a, b))

# after the join we will have (ticker, ((first_close, first_date), (last_close, last_date)))
# we want to flatten as (ticker, (first_close, first_date, last_close, last_date))
join_first_last_date = first_quot_date.join(last_quot_date) \
    .map(lambda x: (x[0], (x[1][0] + x[1][1])))

# then calculate che percent variation and obtain (ticker, (first_date, last_date, perc_var))
percent_variation = join_first_last_date.map(lambda x: (x[0],  # ticker
                                                        (x[1][1],  # first_date
                                                         x[1][3],  # last_date
                                                         calculate_percent_variation(x[1][0], x[1][2]))))

# (ticker, min_price) for each ticker
min_price = remove_first_row.map(lambda line: (line[TICKER], line[MIN])) \
    .reduceByKey(lambda a, b: min(float(a), float(b)))
# (ticker, max_price) for each ticker
max_price = remove_first_row.map(lambda line: (line[TICKER], line[MAX])) \
    .reduceByKey(lambda a, b: max(float(a), float(b)))

# join together all the calculated results, in the form (ticker, (first_date, last_date, percent_var, min, max))
# also it has to be ordered by last_date descending
results = percent_variation.join(min_price) \
    .map(lambda x: (x[0], (x[1][0] + (x[1][1],)))) \
    .join(max_price) \
    .map(lambda x: (x[0], (x[1][0] + (x[1][1],)))) \
    .sortBy(keyfunc=lambda x: x[1][1], ascending=False) \
    .coalesce(1)

# write all (ticker, (results)) pairs in file
results.saveAsTextFile(output_filepath)
