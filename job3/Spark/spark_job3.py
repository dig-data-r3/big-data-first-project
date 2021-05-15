#!/usr/bin/env python3

import argparse
from datetime import datetime
import csv

from pyspark.sql import SparkSession

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

historical_stock_prices = sc.textFile(input_filePath).cache()
