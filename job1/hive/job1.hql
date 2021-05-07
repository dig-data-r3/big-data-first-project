-- Drop tables if exist
DROP TABLE docs;

-- Create the input table
CREATE TABLE docs (ticker STRING, open_price DECIMAL(38,13), close_price DECIMAL(38,13), adj_close DECIMAL(38,13), low DECIMAL(38,13), high DECIMAL(38,13), volume INT, price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Put the dataset into the input table
LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE docs;


-- Visualize the output table
select ticker, min(price_date) as first_price_date, max(price_date) as last_price_date, max(((max_table.max_close_price - min_table.min_close_price) / min_table.min_close_price) * 100) as variation, max(high) as max_price, min(low) as min_price
from docs d
join (
select d.ticker as max_ticker, d.close_price as max_close_price
from docs d
join (select ticker as max_ticker, max(price_date) as max_price_date FROM docs group by ticker) max_table on (d.ticker = max_table.max_ticker and d.price_date >= max_table.max_price_date)
) max_table on d.ticker = max_table.max_ticker
join (
select d.ticker as min_ticker, d.close_price as min_close_price
from docs d
join (select ticker as min_ticker, min(price_date) as min_price_date FROM docs group by ticker) min_table on (d.ticker = min_table.min_ticker and d.price_date <= min_table.min_price_date)
) min_table on d.ticker = min_table.min_ticker
group by ticker
order by last_price_date desc;


-- SPECIFICHE

-- Un job che sia in grado di generare un report contenente, per ciascuna azione:
-- (a) la data della prima quotazione,
-- (b) la data dell’ultima quotazione,
-- (c) la variazione percentuale della quotazione (differenza percentuale tra il primo e l’ultimo prezzo di 	   chiusura presente nell’archivio),
-- (d) il prezzo massimo e quello minimo e
-- (e) (facoltativo) il massimo numero di giorni consecutivi in cui l’azione è cresciuta(chiusura maggiore
--     dell’apertura)con indicazione dell’anno in cui questo è avvenuto. 
-- Il report deve essere ordinato per valori decrescenti del punto b.
