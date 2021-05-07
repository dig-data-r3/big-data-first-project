-- Drop tables if exist
drop table docs1;
drop table docs2;

-- Create the input table 1
CREATE TABLE docs1 (ticker STRING, open_price DECIMAL(15,13), close_price DECIMAL(15,13), adj_close DECIMAL(15,13), low DECIMAL(15,13), high DECIMAL(15,13), volume INT, price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Create the input table 2
CREATE TABLE docs2 (ticker STRING, exchange STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Put the dataset into the input tableS
LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/input/historical_stock_prices.csv' OVERWRITE INTO TABLE docs1;

LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/input/historical_stocks.csv' OVERWRITE INTO TABLE docs2;

select * from docs2 limit 20;


-- SPECIFICHE
-- Un  job  che  sia in  grado  di  generareun reportcontenente, per  ciascun settore  e  per  ciascun anno del  periodo  2009-2018:
-- (a)  la  variazione percentualedella  quotazionedel  settore1nell’anno, (b) l’azione del settore che ha avuto il maggior incremento
-- percentuale nell’anno (con indicazione dell’incremento) e (c) l’azione del settore che ha avuto il maggior  volume  di 
-- transazioninell’anno(con  indicazione  del  volume). Il  report  deve  essere  ordinato  per  nome  del settore. 
