#!/usr/bin/env python3
"""spark application"""
import argparse
from datetime import datetime
import csv

# create parser and set its arguments
from pyspark.sql import SparkSession


# the second dataset needs double-quotes ("") escaping to parse correctly
def parse_line(row):
    csv_reader = csv.reader([row], delimiter=',')
    return next(csv_reader)


def min_date(a, b):
    date_a = datetime.strptime(a[2], '%Y-%m-%d').date()
    date_b = datetime.strptime(b[2], '%Y-%m-%d').date()
    if date_a <= date_b:
        return a
    else:
        return b


def max_date(a, b):
    date_a = datetime.strptime(a[2], '%Y-%m-%d').date()
    date_b = datetime.strptime(b[2], '%Y-%m-%d').date()
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
input_hsp, input_hs, output_filepath = args.input_hsp, args.input_hs, args.output_path

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
# after this point hsp is in the format: (key=(ticker), value=(close_price, volume, date))
hsp = historical_stock_prices.map(lambda line: line.strip().split(',')) \
    .filter(lambda line: line[TICKER] != 'ticker' and int(line[DATE][0:4]) in range(2009, 2018+1)) \
    .map(lambda line: (line[TICKER], (line[CLOSE], line[VOLUME], line[DATE])))

# we need to get the sector for each ticker (other fields are not important)
# hs is formatted as (ticker, sector)
hs = historical_stocks.map(lambda line: parse_line(line)) \
    .filter(lambda line: line[TICKER] != 'ticker') \
    .map(lambda line: (line[TICKER], line[SECTOR]))

# adds the sector to each ticker in hsp via join
# after the join: (ticker, ((close, volume, date), (sector)))
# the tuples get transformed to: (key=(sector, year, ticker), value=(close, volume, date))
hsp_sector = hsp.join(hs) \
    .map(lambda x: ((x[1][1], int(x[1][0][2][0:4]), x[0]), x[1][0]))

# this calculates the first and last quotation dates of each (ticker, year) pair
# it also removes the volume field that remains (correct value calculated further down)
first_quot_date = hsp_sector.reduceByKey(lambda a, b: min_date(a, b)) \
    .map(lambda x: (x[0], (x[1][0], x[1][1])))
last_quot_date = hsp_sector.reduceByKey(lambda a, b: max_date(a, b)) \
    .map(lambda x: (x[0], (x[1][0], x[1][1])))

# after join: ((sector, year, ticker), ((first_close, volume, first_date), (last_close, volume, last_date)))
# we want to flatten as ((sector, year, ticker), (first_close, first_date, last_close, last_date, perc_var))
percent_variation = first_quot_date.join(last_quot_date) \
    .map(lambda x: (x[0], (x[1][0] + x[1][1] + (calculate_percent_variation(x[1][0][0], x[1][1][0]),))))

# sum of all the volumes (line[1][1]) per (sector, year, ticker)
total_volume = hsp_sector.map(lambda line: (line[0], line[1][1])) \
    .reduceByKey(lambda a, b: int(a) + int(b))

# aggregates the sum of volumes and percent variation of a given (sector, ticker, year) pair
# after join: ((sector, ticker, year), ((first_close, first_date, last_close, last_date, perc_var), tot_volume))
# it will result in ((sector, ticker, year), (first_close, first_date, last_close, last_date, percent_var, volume_sum))
ticker_year_results = percent_variation.join(total_volume) \
    .map(lambda x: (x[0], (x[1][0] + (x[1][1],)))) \
    .sortBy(keyfunc=lambda x: x[0][0], ascending=True)

#result =

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
ticker_year_results.coalesce(1).saveAsTextFile(output_filepath)
