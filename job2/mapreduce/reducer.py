#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

# map each ticker for each sector and year to the values required to compute result
#   tickerDataBySectorYear[('AAPL', 'TECH', 2012)] = {
#       'first_close_date': 2012-01-01,
#       'first_close_value': 50.5,
#       'last_close_date': 2012-12-31,
#       'last_close_value': 240,
#       'total_volume': 300000}
# after loading all the values it's useful to compute the percentage variation for the next step
tickerDataBySectorYear = {}

# structure to save the aggregated values from the previous one
#   aggregatedSectorYearData[('TECH', 2012)] = {
#       'sum_initial_close': 4000,
#       'sum_final_close': 6000,
#       'max_perc_var_ticker': 'AAPL',
#       'max_perc_var_value': 75,
#       'max_total_volume_ticker': 'AAPL',
#       'max_total_volume_value': 3000000
#       }
aggregatedSectorYearData = {}


def calculatePercVar(initialValue, finalValue):
    return (finalValue - initialValue) / initialValue * 100


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

    # save (in memory) the info of each ticker per year and sector (inefficient)
    if (ticker, sector, date.year) not in tickerDataBySectorYear:
        newTicker = {'first_close_date': date,
                     'first_close_value': ticker,
                     'last_close_date': date,
                     'last_close_value': ticker,
                     'total_volume': volume}
        tickerDataBySectorYear[(ticker, sector, date.year)] = newTicker
    # the ticker in that year (with that sector) has been seen, update it
    else:
        currTicker = tickerDataBySectorYear[(ticker, sector, date.year)]
        if date < currTicker['first_close_date']:
            currTicker['first_close_date'] = date
            currTicker['first_close_value'] = ticker
        if date > currTicker['last_close_date']:
            currTicker['last_close_date'] = date
            currTicker['last_close_value'] = ticker
        currTicker['total_volume'] += volume

# aggregate the single ticker and year data by sector
for (ticker, sector, year) in tickerDataBySectorYear:
    currTicker = tickerDataBySectorYear[(ticker, sector, year)]
    initialClose = currTicker['first_close_value']
    finalClose = currTicker['last_close_value']
    percVar = calculatePercVar(initialClose, finalClose)
    volume = currTicker['total_volume']
    # create a new dict to save the data
    if (sector, year) not in aggregatedSectorYearData:
        newData = {'sum_initial_close': initialClose,
                   'sum_final_close': finalClose,
                   'max_perc_var_ticker': ticker,
                   'max_perc_var_value': percVar,
                   'max_total_volume_ticker': ticker,
                   'max_total_volume_value': volume}
        aggregatedSectorYearData[(sector, year)] = newData
    # update the existing data
    else:
        currData = aggregatedSectorYearData[(sector, year)]
        currData['sum_initial_close'] += initialClose
        currData['sum_final_close'] += finalClose
        if percVar > currData['maxperc_var_value']:
            currData['max_perc_var_ticker'] = ticker
            currData['max_perc_var_value'] = percVar
        if volume > currData['max_total_volume_value']:
            currData['max_total_volume_ticker'] = ticker
            currData['max_total_volume_value'] = volume


# sort the results based on sector
# aggregatedSectorYearData entries are in the format: ('Sector', Year): {'min': 1, 'max': 2, ...}
sortedResults = sorted(aggregatedSectorYearData.items(),
                       # takes the first argument in (key, value), and the first argument in key=(sector, year)
                       # it will return a list of ordered keys (by their partial value, sector)
                       key=lambda single_entry: single_entry[0][0],
                       # orders the sectors by ascending alphabetical order
                       reverse=False)

# each result has the format of aggregatedSectorYearData entries, plus a new field for the total percent variation
for result in sortedResults:
    sector = result[0][0]
    year = result[0][1]

    currResult = aggregatedSectorYearData[(sector, year)]
    initialCloseSum = currResult['sum_initial_close']
    finalCloseSum = currResult['sum_final_close']
    currResult['total_perc_var'] = calculatePercVar(initialCloseSum, finalCloseSum)

    print('{:<10s}\t{}\t{:>25}%\t{:<10s}\t{:>25}%\t:<10s}\t{}'.format(
        sector,
        year,
        currResult['total_perc_var'],
        currResult['max_perc_var_ticker'],
        currResult['max_perc_var_value'],
        currResult['max_total_volume_ticker'],
        currResult['max_total_volume_value']))

# structure to save the aggregated values from the previous one
#   aggregatedSectorYearData[('TECH', 2012)] = {
#       'sum_initial_close': 4000,
#       'sum_final_close': 6000,
#       'max_perc_var_ticker': 'AAPL',
#       'max_perc_var_value': 75,
#       'max_total_volume_ticker': 'AAPL',
#       'max_total_volume_value': 3000000
#       }
