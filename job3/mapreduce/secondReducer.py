#!/usr/bin/env python3
"""secondReducer.py"""

import sys

# the dict aggregates all months for each ticker
# tickerToMonthsVar = {'AAPL': {0: 13.5, 1: 12.0, ... , 12: -5.3}, ...}
tickerToMonthsVar = {}

# structure to contain the cross product (without duplicates or inverted pairs)
# crossProduct = {('AAPL', 'AMZN'): {}}

# each month is unique for a given ticker (we assume no duplicates)
for row in sys.stdin:
    # parse the input elements
    ticker, month, percVar = row.strip().split("\t")

    # convert the fields in a row
    try:
        month = float(month)
        percVar = float(percVar)
    except ValueError:
        # if one of the fields is not formatted correctly
        continue

    if ticker not in tickerToMonthsVar:
        tickerToMonthsVar[ticker] = {month: percVar}
    else:
        tickerToMonthsVar[ticker][month] = percVar

#
