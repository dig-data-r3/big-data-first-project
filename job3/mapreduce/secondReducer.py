#!/usr/bin/env python3
"""secondReducer.py"""

import sys

THRESHOLD = 1
MONTHS_LITERAL = {1: 'GEN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAG', 6: 'GIU', 7: 'LUG', 8: 'AGO', 9: 'SET',
                  10: 'OTT', 11: 'NOV', 12: 'DIC'}

# the dict aggregates all months for each ticker
# tickerToMonthsVar = {'AAPL': {0: 13.5, 1: 12.0, ... , 12: -5.3}, ...}
tickerToMonthsVar = {}

# structure to contain the cross product (without duplicates or inverted pairs)
# crossProduct = {('AAPL', 'AMZN'): {1: (2, 2.6), 2: (-1, 3.7), ... },
#                 ('AAPL', 'BTP'): {1: (2, -1), 2: (-1, 3.4), ...}}
crossProduct = {}


# generates a merged pair to insert in the crossProduct data structure
# it will return None if the pair of tickers is not similar enough (based on 1% threshold of percVar for each month)
def mergeTickerPair(ticker1, ticker2, tickerData):
    result = {}
    ticker1Data = tickerData[ticker1]
    ticker2Data = tickerData[ticker2]
    # the comparison will fail if the months data are not consistent
    if ticker1Data.keys() != ticker2Data.keys():
        return None
    for month in ticker1Data:
        percVar1 = ticker1Data[month]
        percVar2 = ticker2Data[month]
        month = MONTHS_LITERAL[month]
        if abs(percVar1 - percVar2) <= THRESHOLD:
            result[month] = (percVar1, percVar2)
        else:
            return None
    return result


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

# generates all the possible (and useful) pairs of the cross product
for ticker1 in tickerToMonthsVar:
    for ticker2 in tickerToMonthsVar:
        if (ticker1, ticker2) in crossProduct or (ticker2, ticker1) in crossProduct or ticker1 == ticker2:
            continue
        else:
            mergedPair = mergeTickerPair(ticker1, ticker2, tickerToMonthsVar)
            if mergedPair is None:
                continue
            else:
                crossProduct[(ticker1, ticker2)] = mergedPair

for pair in crossProduct:
    print('{}\t{}'.format(
        pair,
        crossProduct[pair]))
