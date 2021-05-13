-- Drop output table if it exists
DROP TABLE job1_hive;


-- For each ticker -> its last close price in the dataset

create table ticker_to_maxDate as
select d.ticker as max_ticker, d.close_price as max_close_price
from historical_stock_prices d
join (select ticker as max_ticker, max(price_date) as max_price_date
FROM historical_stock_prices group by ticker) as max_table
on (d.ticker = max_table.max_ticker and d.price_date >= max_table.max_price_date);


-- For each ticker -> its first close price in the dataset

create table ticker_to_minDate as
select d.ticker as min_ticker, d.close_price as min_close_price
from historical_stock_prices d
join (select ticker as min_ticker, min(price_date) as min_price_date
FROM historical_stock_prices group by ticker) min_table
on (d.ticker = min_table.min_ticker and d.price_date <= min_table.min_price_date);


-- Saves the output table in hdfs path /users/hive/warehouse/
-- Create results table
CREATE TABLE job1_hive ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n' as
select ticker, min(price_date) as first_price_date, max(price_date) as last_price_date, max(((max_table.max_close_price - min_table.min_close_price) / min_table.min_close_price) * 100) as variation, max(high) as max_price, min(low) as min_price
from historical_stock_prices d
join ticker_to_maxDate max_table on d.ticker = max_table.max_ticker
join ticker_to_minDate min_table on d.ticker = min_table.min_ticker
group by ticker
order by ticker, last_price_date desc;


-- Show results
select * from job1_hive;


-- Drop useless tables
drop table ticker_to_maxDate;
drop table ticker_to_minDate;



-- SPECIFICHE

-- Un job che sia in grado di generare un report contenente, per ciascuna azione:
-- (a) la data della prima quotazione,
-- (b) la data dell’ultima quotazione,
-- (c) la variazione percentuale della quotazione (differenza percentuale tra il primo e l’ultimo prezzo di chiusura presente nell’archivio),
-- (d) il prezzo massimo e quello minimo e
-- (e) (facoltativo) il massimo numero di giorni consecutivi in cui l’azione è cresciuta (chiusura maggiore
--     dell’apertura)con indicazione dell’anno in cui questo è avvenuto.
-- Il report deve essere ordinato per valori decrescenti del punto b.
