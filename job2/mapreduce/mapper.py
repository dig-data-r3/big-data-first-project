#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv
from datetime import datetime

ticker_to_sector = {}

# process the historical stocks database (already cleaned from null sectors) before the join
with open('historical_stocks_clean.csv') as hs_file:
    # csv.reader reads the fields with a comma inside " " correctly (company name)
    csv_reader = csv.reader(hs_file, delimiter=',')
    is_first_line = True
    for row in csv_reader:
        if is_first_line:
            # ignore csv header information
            is_first_line = False
        else:
            ticker, _, _, sector, _ = row
            ticker_to_sector[ticker] = sector

# job2 requires a specific time frame to consider
START_YEAR = 2009
END_YEAR = 2018

is_first_line = True

# read lines from STDIN (historical_stock_prices dataset)
for row in sys.stdin:
    # ignores the first row, which contains column names
    if is_first_line:
        is_first_line = False
        continue

    # split the current row into fields (ignoring not needed ones)
    ticker, _, closePrice, _, _, _, volume, date = row.strip().split(',')
    date = datetime.strptime(date, '%Y-%m-%d').date()

    # the ticker had a null sector, ignore it
    if ticker not in ticker_to_sector:
        continue

    # write the separated fields to standard output, applying the necessary filtering
    if START_YEAR <= date.year <= END_YEAR:
        # the join adds a column sector
        sector = ticker_to_sector[ticker]
        print('{}\t{}\t{}\t{}\t{}'.format(
            # shuffle and sort could be useful on these first three columns
            sector,
            ticker,
            date,
            closePrice,
            volume))
