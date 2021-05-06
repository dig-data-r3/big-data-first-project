#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

# maps each (sector, year) to the required values to calculate
# for example:
#   {('FINANCE', 2010): {
#       'year_quot_perc_var': 13.6,
#       'max_perc_incr_ticker': 'AMZN',
#       'max_perc_incr_value': 25.3,
#       'max_vol_incr_ticker': 'AAPL',
#       'max_vol_incr_value': 2000}}
results = {}



# input comes from STDIN
for line in sys.stdin:
    # remove leading/trailing spaces
    line = line.strip()
    # parse the input elements
    sector, ticker, date, closePrice, volume = line.split("\t")

    # convert the fields in a row
    try:
        closePrice = float(closePrice)
        volume = float(volume)
        date = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        # if one of the numerical fields is not formatted correctly
        continue

    # if the (sector, year) hasn't been seen before, initialize its values
    if (sector, date.year) not in results:
        #   {('FINANCE', 2010): {
        #       'year_quot_perc_var': 13.6,
        #       'max_perc_incr_ticker': 'AMZN',
        #       'max_perc_incr_value': 25.3,
        #       'max_vol_incr_ticker': 'AAPL',
        #       'max_vol_incr_value': 2000}}

        results[(sector, date.year)] = {
            'first_quot_date': date,
            'last_quot_date': date,
            'first_quot_price': closePrice,
            'last_quot_price': closePrice,
            'perc_var': 0,
            'min_price': minPrice,
            'max_price': maxPrice
        }

    currTicker = results[ticker]

    # update the current input ticker
    if date < results[ticker]['first_quot_date']:
        currTicker['first_quot_date'] = date
        currTicker['first_quot_price'] = closePrice
    if date > results[ticker]['last_quot_date']:
        currTicker['last_quot_date'] = date
        currTicker['last_quot_price'] = closePrice
    if minPrice < results[ticker]['min_price']:
        currTicker['min_price'] = minPrice
    if maxPrice > results[ticker]['max_price']:
        currTicker['max_price'] = maxPrice

# sort the results from the most recent to old quotation dates
sortedResults = sorted(results.items(),
                       # result is in the format ('TickerName', {'min': 1, 'max': 2}), a tuple
                       key=lambda single_result: single_result[1]['last_quot_date'],
                       reverse=True)

# result is in the format ('TickerName', {'min': 1, 'max': 2}), a tuple
for result in sortedResults:
    tickerName = result[0]
    tickerResults = result[1]
    tickerResults['perc_var'] = (tickerResults['last_quot_price'] - tickerResults['first_quot_price']) / \
        tickerResults['first_quot_price'] * 100
    print('{:<10s}\t{}\t{}\t{:>25}%\t{:<20}\t{:<20}'.format(
        tickerName,
        tickerResults['first_quot_date'],
        tickerResults['last_quot_date'],
        tickerResults['perc_var'],
        tickerResults['min_price'],
        tickerResults['max_price']))
