#!/usr/bin/env python3
"""firstMapper.py"""

import sys
from datetime import datetime

is_first_line = True

# read lines from STDIN
for row in sys.stdin:
    # ignores the first row, which contains column names
    if is_first_line:
        is_first_line = False
        continue

    # split the current row into fields (ignoring not needed ones)
    ticker, _, closePrice, _, _, _, _, date = row.strip().split(',')
    date = datetime.strptime(date, '%Y-%m-%d').date()

    # filter out all the years but 2017
    if date.year != 2017:
        continue

    # write the separated fields to standard output
    print('{}\t{}\t{}'.format(
        ticker,
        closePrice,
        date))
