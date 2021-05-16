#!/usr/bin/env python3

import argparse
from datetime import datetime

from pyspark.sql import SparkSession


def min_price_and_date(x, y):
    x_price = x[1]
    y_price = y[1]
    x_date = x[2]
    y_date = y[2]

    if x_date <= y_date:
        return float(x_price)
    else:
        return float(y_price)

def max_price_and_date(x, y):
    x_price = x[1]
    y_price = y[1]
    x_date = x[2]
    y_date = y[2]

    if x_date >= y_date:
        return float(x_price)
    else:
        return float(y_price)


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


historical_stock_prices = sc.textFile(input_filepath).cache()

split_input = historical_stock_prices.map(lambda line: line.strip().split(","))

remove_first_row = split_input.filter(lambda line: line[0] != 'ticker')

split_input_datetime = remove_first_row.map(lambda x: (x[0], x[2], datetime.strptime(x[7], "%Y-%m-%d").date()))

data_2017 = split_input_datetime.filter(lambda x: x[2].year == 2010)

ticker_month_to_list = data_2017.map(lambda x: ((x[0], x[2].month), x))

ticker_month_to_mindate = ticker_month_to_list.reduceByKey(min_price_and_date)

ticker_month_to_maxdate = ticker_month_to_list.reduceByKey(max_price_and_date)

ticker_month_variation = ticker_month_to_mindate.join(ticker_month_to_maxdate).map(lambda x: (x[0], ((x[1][1]-x[1][0])/x[1][0])*100))


ticker_pairs_trashold = ticker_month_variation.cartesian(ticker_month_variation).filter(lambda x: abs(x[0][1] - x[1][1]) <= 100 and x[0][0][1]==x[1][0][1] and x[0][0][0] < x[1][0][0])


ticker_pair_to_variations = ticker_pairs_trashold.map(lambda x: ((x[0][0][0], x[1][0][0]), x[0][0][1], x[0][1], x[1][1])).foreach(print)

