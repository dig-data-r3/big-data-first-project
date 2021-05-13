#!/usr/bin/env python3
"""spark application"""
import argparse
from datetime import datetime

# create parser and set its arguments
from pyspark.sql import SparkSession


def min_date(a, b):
    date_a = datetime.strptime(a, '%Y-%m-%d').date()
    date_b = datetime.strptime(b, '%Y-%m-%d').date()
    if date_a <= date_b:
        return a
    else:
        return b


def max_date(a, b):
    date_a = datetime.strptime(a, '%Y-%m-%d').date()
    date_b = datetime.strptime(b, '%Y-%m-%d').date()
    if date_a >= date_b:
        return a
    else:
        return b


def calculate_percent_variation(initial, final):
    return (float(final) - float(initial)) / float(initial) * 100


# fields' index in a row (in historical_stock_prices.csv)
TICKER = 0
OPEN = 1
CLOSE = 2
ADJ_CLOSE = 3
MIN = 4
MAX = 5
VOLUME = 6
DATE = 7

# fields' index in a row (historical_stocks.csv)
TICKER = 0
EXCHANGE = 1
NAME = 2
SECTOR = 3
INDUSTRY = 4

parser = argparse.ArgumentParser()
parser.add_argument("--input_hsp", type=str, help="Input file historical stock prices")
parser.add_argument("--input_hs", type=str, help="Input file historical stocks")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_hsp, input_hs, output_filepath = args.input_hsp, args.input_hsp, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job2 Spark") \
    .getOrCreate()

sc = spark.sparkContext

# spark.sparkContext.textFile(filepath) returns an RDD
# with a record for each line in the input file
historical_stock_prices = sc.textFile(input_hsp).cache()
# this dataset was already cleaned from null sectors
historical_stocks = sc.textFile(input_hs).cache()

# job2 requires to only consider years 2009-2018
# after this point hsp is in the format: (key=(ticker, year), value=(close_price, volume, date))
hsp = historical_stock_prices.map(lambda line: line.strip().split(',')) \
    .filter(lambda line: line[TICKER] != 'ticker' and int(line[DATE][0:4]) in range(2009, 2018+1)) \
    .map(lambda line: ((line[TICKER], int(line[DATE][0:4])), (line[CLOSE], line[VOLUME], line[DATE])))

# we need to get the sector for each ticker (other fields are not important)
hs = historical_stocks.map(lambda line: line.strip().split(',')) \
    .filter(lambda line: line[TICKER] != 'ticker') \
    .map(lambda line: (line[TICKER], line[SECTOR]))

# this calculates the first and last quotation dates of each (ticker, year) pair
first_quot_date = hsp.reduceByKey(lambda a, b: min_date(a[2], b[2]))
last_quot_date = hsp.reduceByKey(lambda a, b: max_date(a[2], b[2]))

# sum of all the volumes (line[1][1]) per (ticker, year)
total_volume = hsp.map(lambda line: (line[0], line[1][1])) \
    .reduceByKey(lambda a, b: a + b)

# after the join we will have ((ticker, year), ((first_close, first_date), (last_close, last_date)))
# we want to flatten as ((ticker, year), (first_close, first_date, last_close, last_date))
join_first_last_date = first_quot_date.join(last_quot_date) \
    .map(lambda x: (x[0], (x[1][0] + x[1][1])))

# then calculate che percent variation and obtain ((ticker, year), (first_date, last_date, percent_variation))
percent_variation = join_first_last_date.map(lambda x: (x[0],  # (ticker, year)
                                                        (x[1][1],  # first_date
                                                         x[1][3],  # last_date
                                                         calculate_percent_variation(x[1][0], x[1][2]))))


# # join together all the calculated results, in the form (ticker, (first_date, last_date, percent_var, min, max))
# # also it has to be ordered by last_date descending
# results = percent_variation.join(min_price) \
#     .map(lambda x: (x[0], (x[1][0] + (x[1][1],)))) \
#     .join(max_price) \
#     .map(lambda x: (x[0], (x[1][0] + (x[1][1],)))) \
#     .sortBy(keyfunc=lambda x: x[1][1], ascending=False) \
#     .coalesce(1)  # avoids having multiple (even hundreds) of part-* files as output (only one part-00000)
#
# # write all (ticker, (results)) pairs in file
# results.saveAsTextFile(output_filepath)
