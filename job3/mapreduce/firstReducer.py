#!/usr/bin/env python3
"""firstReducer.py"""

import sys
from datetime import datetime

# saves the monthly first and last close price for each ticker (along with their dates for comparing)
# tickerToMonthVar = {'AAPL':
#                       { 1: {'first_close': 15, 'last_close': 20, 'first_date': .., 'last_date': ..},
#                         2: {'first_close': 20, 'last_close': 2, ...}
#                         ...
#                         12: {'first_close': 5, 'last_close': 2, ...} ...}
tickerToMonthVar = {}


def calculatePercVar(initialValue, finalValue):
    return (finalValue - initialValue) / initialValue * 100


# input comes from STDIN (the previous mapper)
for row in sys.stdin:
    # parse the input elements
    ticker, closePrice, date = row.strip().split("\t")

    # convert the fields in a row
    try:
        closePrice = float(closePrice)
        date = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        # if one of the fields is not formatted correctly
        continue

    # insert ticker data in the dict
    if (ticker not in tickerToMonthVar) or (date.month not in tickerToMonthVar[ticker]):
        currTickerMonth = {'first_close': closePrice,
                           'last_close': closePrice,
                           'first_date': date,
                           'last_date': date}
        tickerToMonthVar[ticker][date.month] = currTickerMonth
    # the ticker and month are already in the dict, update them
    else:
        currTickerMonth = tickerToMonthVar[ticker][date.month]
        if date < currTickerMonth['first_date']:
            currTickerMonth['first_close'] = closePrice
            currTickerMonth['first_date'] = date
        if date > currTickerMonth['last_date']:
            currTickerMonth['last_close'] = closePrice
            currTickerMonth['last_date'] = date

    # print the data structure calculating the monthly percent variation
    for ticker in tickerToMonthVar:
        monthData = tickerToMonthVar[ticker]
        # iterate over all months for the ticker
        for month in monthData:
            initialClose = monthData['first_close']
            finalClose = monthData['last_close']
            # prints ('AAPL', 3, 25.3) separating each month for the same ticker (put together in next mapreduce)
            print('{}\t{}\t{}'.format(ticker, month, calculatePercVar(initialClose, finalClose)))

