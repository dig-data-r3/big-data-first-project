-- Drop tables if exist
drop table docs1;
drop table docs2;

-- Create the input table 1
CREATE TABLE docs1 (ticker STRING, open_price DECIMAL(38,13), close_price DECIMAL(38,13), adj_close DECIMAL(38,13), low DECIMAL(38,13), high DECIMAL(38,13), volume INT, price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Create the input table 2
CREATE TABLE docs2 (ticker STRING, exch STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
);


-- Put the dataset into input tables
LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE docs1;

LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/dataset/historical_stocks.csv' OVERWRITE INTO TABLE docs2;

-- sector to year
create table sector_2_date as
select distinct d2.sector, extract(year from d1.price_date)
from docs1 as d1 left join docs2 as d2 on d1.ticker = d2.ticker
order by d2.sector, `_c1`;

alter table sector_2_date change `_c1` year int;

--sector to min/max date
create table sector_min_max as
select d2.sector, sd.year, min(d1.price_date) as first_date, max(d1.price_date) as last_date
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_2_date as sd on d2.sector = sd.sector and sd.year = extract(year from d1.price_date)
where sd.year >=2009 and sd.year <= 2018
group by d2.sector, sd.year
order by sector, year;

-- sector to first quotation
create table sector_to_min_quot as
select d2.sector, sm.year, sum(d1.close_price) as first_quot
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_min_max as sm on d2.sector = sm.sector and sm.year = extract(year from d1.price_date)
where d1.price_date = sm.first_date
group by d2.sector, sm.year
order by d2.sector, sm.year;

-- sector to last quotation
create table sector_to_max_quot as
select d2.sector, sm.year, sum(d1.close_price) as last_quot
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_min_max as sm on d2.sector = sm.sector and sm.year = extract(year from d1.price_date)
where d1.price_date = sm.last_date
group by d2.sector, sm.year
order by d2.sector, sm.year;


-- Visualize the output table
-- working on (b)
--select ticker, max(((max_table.max_close_price - min_table.min_close_price) / min_table.min_close_price) * 100) as variation, price_date
--from docs d
--join (
--select d.ticker as max_ticker, d.close_price as max_close_price
--from docs1 d
--join sector_min_max as smm on (d.ticker = smm.ticker and d.price_date = smm.last_date)
--) max_table on d.ticker = max_table.max_ticker
--join (
--select d.ticker as min_ticker, d.close_price as min_close_price
--from docs d
--join (select ticker as min_ticker, min(price_date) as min_price_date FROM docs group by ticker) min_table on (d.ticker = min_table.min_ticker and d.price_date <= min_table.min_price_date)
--) min_table on d.ticker = min_table.min_ticker
--group by ticker, price_date;



--results
select d2.sector, smin.year, min(((smax.last_quot - smin.first_quot)/smin.first_quot)*100) as variation
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_to_min_quot as smin on d2.sector = smin.sector and smin.year = extract(year from d1.price_date)
left join sector_to_max_quot as smax on d2.sector = smax.sector and smax.year = extract(year from d1.price_date)
where smin.year >=2009 and smin.year <= 2018 and smax.year >=2009 and smax.year <= 2018
group by d2.sector, smin.year
order by d2.sector, smin.year;



-- SPECIFICHE
-- Un  job  che  sia in  grado  di  generare un report contenente, per ciascun settore  e  per  ciascun anno del  periodo  2009-2018:
-- (a) la  variazione percentuale della  quotazione del settore nell’anno
-- (b) l’azione del settore che ha avuto il maggior incremento percentuale nell’anno (con indicazione dell’incremento) e
-- (c) l’azione del settore che ha avuto il maggior  volume  di transazioninell’anno(con  indicazione  del  volume).
-- Il report  deve  essere  ordinato  per  nome  del settore.

--CONSUMER DURABLES	2009	54.887800
--CONSUMER DURABLES	2009	40.518400

--select d1.price_date, sum(d1.close_price)
--from docs1 d1 left join docs2 d2 on d1.ticker = d2.ticker
--where d2.sector="CONSUMER DURABLES" and extract(year from d1.price_date)=2010
--group by d1.price_date
--order by d1.price_date;

-- 2009-01-02	868.4795521497730
-- 2009-12-31	1345.1686422973870
-- ((13451686422973870 − 8684795521497730) ÷ 8684795521497730) × 100 = 54.887800