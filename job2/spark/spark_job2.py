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


def min_value(a, b, val_type, index):
    if val_type == 'date':
        val_a = datetime.strptime(a[index], '%Y-%m-%d').date()
        val_b = datetime.strptime(b[index], '%Y-%m-%d').date()
    else:
        val_a = float(a[index])
        val_b = float(b[index])
    if val_a <= val_b:
        return a
    else:
        return b


def max_value(a, b, val_type, index):
    if val_type == 'date':
        val_a = datetime.strptime(a[index], '%Y-%m-%d').date()
        val_b = datetime.strptime(b[index], '%Y-%m-%d').date()
    else:
        val_a = float(a[index])
        val_b = float(b[index])
    if val_a >= val_b:
        return a
    else:
        return b


def calculate_percent_variation(initial, final):
    return (float(final) - float(initial)) / float(initial) * 100


def sum_tuple(a, b):
    val_a = (float(a[0]), float(a[1]))
    val_b = (float(b[0]), float(b[1]))
    sum_tuples = [sum(i) for i in zip(*(val_a, val_b))]
    return tuple(sum_tuples)


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

# this calculates the first and last quotation dates (with values) of each (sector, ticker, year) tuple
# each of these RDDs returns: ((sector, year, ticker), (close))
first_quotation_close = hsp_sector.reduceByKey(lambda a, b: min_value(a, b, 'date', 2)) \
    .map(lambda x: (x[0], x[1][0]))
last_quotation_close = hsp_sector.reduceByKey(lambda a, b: max_value(a, b, 'date', 2)) \
    .map(lambda x: (x[0], x[1][0]))

# after join: ((sector, year, ticker), (first_close, last_close))
# we add the percent variation: ((sector, year, ticker), (first_close, last_close, percent_var))
ticker_percent_variation = first_quotation_close.join(last_quotation_close) \
    .map(lambda x: (x[0], (x[1] + (calculate_percent_variation(x[1][0], x[1][1]), ))))

# calculates the ticker with maximum percent increase for each (sector, year)
# outputs: ((sector, year), (max_ticker, percent_increase))
ticker_max_percent_var = ticker_percent_variation.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1][2]))) \
    .reduceByKey(lambda a, b: max_value(a, b, 'float', 1))

# sums of all the volumes' sum (line[1][1]) per (sector, year, ticker)
# then it returns the ticker that has the max volume: ((sector, year), (max_ticker, volume))
ticker_max_volume = hsp_sector.map(lambda line: (line[0], line[1][1])) \
    .reduceByKey(lambda a, b: int(a) + int(b)) \
    .map(lambda line: ((line[0][0], line[0][1]), (line[0][2], line[1]))) \
    .reduceByKey(lambda a, b: max_value(a, b, 'int', 1))

# input: ((sector, year, ticker), (first_close, last_close, percent_var))
# the first map removes the percent_var from the value of the line, and the ticker from the key
# output: ((sector, year), (sector_year_percent_var))
sector_year_percent_variation = ticker_percent_variation.map(lambda x: ((x[0][0], x[0][1]), (x[1][0], x[1][1]))) \
    .reduceByKey(lambda a, b: sum_tuple(a, b)) \
    .map(lambda x: (x[0], calculate_percent_variation(x[1][0], x[1][1])))

# aggregates the sector variation and the tickers with max variation and volume for a given (sector, year) pair
# after first join: ((sector, year), (sector_percent_var, (max_var_ticker, percent_increase)))
# after second join: ((sector, year), ((sector_var, ticker, percent_increase), (max_ticker, volume)))
# it will result in ((sector, year), (sector_var, ticker, percent_increase, ticker, volume))
results = sector_year_percent_variation.join(ticker_max_percent_var) \
    .map(lambda x: (x[0], ((x[1][0], ) + (x[1][1])))) \
    .join(ticker_max_volume) \
    .map(lambda x: (x[0], (x[1][0] + x[1][1]))) \
    .sortBy(keyfunc=lambda x: x[0], ascending=True) \
    .coalesce(1)

# # write all ((sector, year), (results...)) in a file
results.saveAsTextFile(output_filepath)
