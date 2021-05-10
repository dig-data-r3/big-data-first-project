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


-- For each sector -> year of the date_price

create table sector_2_date as
select distinct d2.sector, extract(year from d1.price_date)
from docs1 as d1 left join docs2 as d2 on d1.ticker = d2.ticker
order by d2.sector, `_c1`;

alter table sector_2_date change `_c1` year int;


-- For each sector and year -> date of the first quotation of the year, date of the last quotation of the year

create table sector_min_max as
select d2.sector, sd.year, min(d1.price_date) as first_date, max(d1.price_date) as last_date
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_2_date as sd on d2.sector = sd.sector and sd.year = extract(year from d1.price_date)
where sd.year >=2009 and sd.year <= 2018
group by d2.sector, sd.year
order by sector, year;


-- For each sector and year -> sum of all the quotations in the first date for that year

create table sector_to_min_quot as
select d2.sector, sm.year, sum(d1.close_price) as first_quot
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_min_max as sm on d2.sector = sm.sector and sm.year = extract(year from d1.price_date)
where d1.price_date = sm.first_date
group by d2.sector, sm.year
order by d2.sector, sm.year;


-- For each sector and year -> sum of all the quotations in the last date for that year

create table sector_to_max_quot as
select d2.sector, sm.year, sum(d1.close_price) as last_quot
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_min_max as sm on d2.sector = sm.sector and sm.year = extract(year from d1.price_date)
where d1.price_date = sm.last_date
group by d2.sector, sm.year
order by d2.sector, sm.year;



------------------------
-- Usefull for task  (b)
------------------------

create table sector_year_to_tickerFirstQuotation as
select d2.sector, sm.year, d1.ticker, close_price as first_quotation
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_min_max as sm on d2.sector = sm.sector
where d1.price_date = sm.first_date
order by d2.sector, sm.year;


create table sector_year_to_tickerLastQuotation as
select d2.sector, sm.year, d1.ticker, close_price as last_quotation
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_min_max as sm on d2.sector = sm.sector
where d1.price_date = sm.last_date
order by d2.sector, sm.year;


create table sector_year_to_tickerFirstLastQuotation as
select s1.sector, s1.year, s1.ticker, s1.first_quotation, s2.last_quotation
from sector_year_to_tickerFirstQuotation as s1
left join sector_year_to_tickerLastQuotation as s2
on (s1.sector = s2.sector and s1.year = s2.year and s1.ticker = s2.ticker)
order by s1.sector, s1.year;


create table sector_year_to_variation as
select sector, year, ticker, max(((last_quotation - first_quotation)/first_quotation)*100) as variation
from sector_year_to_tickerFirstLastQuotation
group by sector, year, ticker;


create table sector_year_to_maxVariation as
select sector, year, max(variation) as max_variation
from sector_year_to_variation
group by sector, year;


create table sector_year_to_maxTicker as
select smax.sector, smax.year, sv.ticker, smax.max_variation
from sector_year_to_maxVariation as smax
left join sector_year_to_variation as sv on smax.sector = sv.sector and smax.year = sv.year
where max_variation = variation;


-----------------------
-- Usefull for task (c)
-----------------------


create table sector_year_ticker_to_volumeSum as
select sy.sector, sy.year, d1.ticker, sum(d1.volume) as volume
from docs1 as d1
right join sector_year_to_maxTicker as sy on d1.ticker = sy.ticker
group by sy.sector, sy.year, d1.ticker;

create table sector_year_to_maxVolume as
select sector, year, max(volume) as maxVolume
from sector_year_ticker_to_volumeSum
group by sector, year
order by sector, year;

create table sector_year_toMaxVolumeTicker as
select ayt.sector, ayt.year, ayt.ticker as v_ticker, ayt.volume
from sector_year_ticker_to_volumeSum as ayt
left join sector_year_to_maxVolume as aym on ayt.sector = aym.sector and ayt.year = aym.year
where volume = maxVolume;





-- Show results

select d2.sector, smin.year, min(((smax.last_quot - smin.first_quot)/smin.first_quot)*100) as variation, max(sy.ticker), max(sy.max_variation), min(v_ticker), max(syv.volume)
from docs1 as d1
left join docs2 as d2 on d1.ticker = d2.ticker
left join sector_to_min_quot as smin on d2.sector = smin.sector and smin.year = extract(year from d1.price_date)
left join sector_to_max_quot as smax on d2.sector = smax.sector and smax.year = extract(year from d1.price_date)
left join sector_year_to_maxTicker sy on d2.sector = sy.sector and sy.year = extract(year from d1.price_date)
left join sector_year_toMaxVolumeTicker as syv on d2.sector = syv.sector and syv.year = extract(year from d1.price_date)
where smin.year >=2009 and smin.year <= 2018 and smax.year >=2009 and smax.year <= 2018 and d2.sector != "N/A"
group by d2.sector, smin.year
order by d2.sector, smin.year;



-- SPECIFICHE
-- Un  job  che  sia in  grado  di  generare un report contenente, per ciascun settore  e  per  ciascun anno del  periodo  2009-2018:
-- (a) la  variazione percentuale della  quotazione del settore nell’anno
-- (b) l’azione del settore che ha avuto il maggior incremento percentuale nell’anno (con indicazione dell’incremento) e
-- (c) l’azione del settore che ha avuto il maggior  volume  di transazioni nell’anno (con  indicazione  del  volume).
-- Il report  deve  essere  ordinato  per  nome  del settore.



-- Query per verificare a mano se i risultati del punto (a) sono giusti
-- stampa per il 2010 e per il settore CONSUMER DURABLES la somma delle quotazioni in ogni data

--select d1.price_date, sum(d1.close_price)
--from docs1 d1 left join docs2 d2 on d1.ticker = d2.ticker
--where d2.sector="CONSUMER DURABLES" and extract(year from d1.price_date)=2010
--group by d1.price_date
--order by d1.price_date;

-- esempio

-- Output from Results
-- CONSUMER DURABLES	2009	12.987500

-- Output dalla query di verifica
-- 2010-12-31	2500.1176440045238
-- 2010-01-04	2212.7374958395961
-- ((2500,1176440045238 − 2212,7374958395961) ÷ 2212,7374958395961) × 100 = 12,987539132

drop table sector_2_date;
drop table sector_min_max;
drop table sector_to_min_quot;
drop table sector_to_max_quot;
drop table sector_year_to_tickerFirstQuotation;
drop table sector_year_to_tickerLastQuotation;
drop table sector_year_to_tickerFirstLastQuotation;
drop table sector_year_to_variation;
drop table sector_year_to_maxVariation;
drop table sector_year_to_maxTicker;
drop table sector_year_ticker_to_volumeSum;
drop table sector_year_to_maxVolume;
drop table sector_year_toMaxVolumeTicker;
