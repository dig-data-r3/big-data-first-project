#!/usr/bin/env python3
"""secondMapper.py"""

import sys

for line in sys.stdin:
    ticker, month, percVar = line.strip().split('\t')
    print('{}\t{}\t{}'.format(ticker, month, percVar))
