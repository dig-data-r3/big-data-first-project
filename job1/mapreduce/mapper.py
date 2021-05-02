#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN (standard input)
for row in sys.stdin:
    # removing leading/trailing whitespaces
    row = row.strip()

    # ignores the first row, which contains column names
    if 'ticker,open,' in row:
        continue

    # split the current row into fields (ignoring not needed ones)
    ticker, _, closePrice, _, minPrice, maxPrice, _, date = row.split(',')

    # write the separated fields to standard output
    print('%s\t%s\t%s\t%s\t%s' % (ticker, closePrice, minPrice, maxPrice, date))

# job1:
# per ciascuna azione:
# 	- data prima quotazione
# 	- data ultima quotazione
# 	- variazione percentuale della quotazione (tra primo e ultimo prezzo di
#   	chiusura nel dataset)
# 	- prezzo massimo e minimo
# 	- (facoltativo) max numero giorni consecutivi in cui azione è cresciuta
#  	    (chiusura > apertura) con l'anno in cui è avvenuto
# ordinare il report per valori decrescenti del secondo punto
# (dalla data di quotazione più recente alla più vecchia)
