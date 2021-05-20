#!/usr/bin/env python3

import pandas as pd


def sample_all_sizes(dataset):
    for size in dataset_sizes:
        n_rows = round(dataset.shape[0] * size)
        sampled_df = dataset.sample(n=n_rows, random_state=42, replace=True)
        filename = 'dataset/historical_stock_prices{}.csv'.format(int(size*2048))
        sampled_df.to_csv(filename, index=False)


dataset_sizes = [0.125, 0.25, 0.5, 2]
stock_prices = pd.read_csv('dataset/historical_stock_prices.csv')
sample_all_sizes(stock_prices)
