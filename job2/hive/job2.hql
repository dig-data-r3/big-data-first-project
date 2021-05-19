-- Drop tables if exist
DROP TABLE job2_hive;


------------------------
-- Usefull for task  (b)
------------------------

-- For each sector -> year of the price date

create table sector_2_date as
select distinct d2.sector, extract(year from d1.price_date)
from ${hivevar:DATASET_NAMESIZE} as d1 left join historical_stocks_clean as d2 on d1.ticker = d2.ticker
order by d2.sector, `_c1`;

alter table sector_2_date change `_c1` year int;


-- For each sector,year and ticker -> date of the first quotation of the year, date of the last quotation of the year

create table sector_ticker_min_max as
select d2.sector, sd.year, d1.ticker, min(d1.price_date) as first_date, max(d1.price_date) as last_date
from ${hivevar:DATASET_NAMESIZE} as d1
left join historical_stocks_clean as d2 on d1.ticker = d2.ticker
left join sector_2_date as sd on d2.sector = sd.sector and sd.year = extract(year from d1.price_date)
where sd.year >=2009 and sd.year <= 2018
group by d2.sector, sd.year, d1.ticker
order by sector, year, d1.ticker;


-- For each sector and year -> sum of all the quotations in the first date for that year

create table sector_to_min_quot as
select d2.sector, sm.year, sum(d1.close_price) as first_quot
from ${hivevar:DATASET_NAMESIZE} as d1
left join historical_stocks_clean as d2 on d1.ticker = d2.ticker
join sector_ticker_min_max as sm on d2.sector = sm.sector and sm.year = extract(year from d1.price_date) and d1.ticker = sm.ticker
where d1.price_date = sm.first_date
group by d2.sector, sm.year
order by d2.sector, sm.year;


-- For each sector and year -> sum of all the quotations in the last date for that year

create table sector_to_max_quot as
select d2.sector, sm.year, sum(d1.close_price) as last_quot
from ${hivevar:DATASET_NAMESIZE} as d1
left join historical_stocks_clean as d2 on d1.ticker = d2.ticker
join sector_ticker_min_max as sm on d2.sector = sm.sector and sm.year = extract(year from d1.price_date) and d1.ticker = sm.ticker
where d1.price_date = sm.last_date and  d2.sector != "N/A"
group by d2.sector, sm.year
order by d2.sector, sm.year;


------------------------
-- Usefull for task  (b)
------------------------

-- For each sector and year -> ticker with its first quotation for that year and that sector

create table sector_year_to_tickerFirstQuotation as
select d2.sector, sm.year, d1.ticker, close_price as first_quotation
from ${hivevar:DATASET_NAMESIZE} as d1
left join historical_stocks_clean as d2 on d1.ticker = d2.ticker
left join sector_ticker_min_max as sm on d2.sector = sm.sector and d1.ticker = sm.ticker
where d1.price_date = sm.first_date
order by d2.sector, sm.year;


-- For each sector and year -> ticker with its last quotation for that year and that sector

create table sector_year_to_tickerLastQuotation as
select d2.sector, sm.year, d1.ticker, close_price as last_quotation
from ${hivevar:DATASET_NAMESIZE} as d1
left join historical_stocks_clean as d2 on d1.ticker = d2.ticker
left join sector_ticker_min_max as sm on d2.sector = sm.sector and d1.ticker = sm.ticker
where d1.price_date = sm.last_date
order by d2.sector, sm.year;


-- For each sector and year -> ticker with its first and last quotation for that year and that sector

create table sector_year_to_tickerFirstLastQuotation as
select s1.sector, s1.year, s1.ticker, s1.first_quotation, s2.last_quotation
from sector_year_to_tickerFirstQuotation as s1
left join sector_year_to_tickerLastQuotation as s2
on (s1.sector = s2.sector and s1.year = s2.year and s1.ticker = s2.ticker)
order by s1.sector, s1.year;


-- For each sector, year and ticker -> ticker price variation in that year for that sector

create table sector_year_to_variation as
select sector, year, ticker, max(((last_quotation - first_quotation)/first_quotation)*100) as variation
from sector_year_to_tickerFirstLastQuotation
group by sector, year, ticker;


-- For each sector and year -> the max price variation for that sector in that year

create table sector_year_to_maxVariation as
select sector, year, max(variation) as max_variation
from sector_year_to_variation
group by sector, year;


-- For each sector and year -> the ticker with the max price variation for that sector in that year

create table sector_year_to_maxTicker as
select smax.sector, smax.year, sv.ticker, smax.max_variation
from sector_year_to_maxVariation as smax
left join sector_year_to_variation as sv on smax.sector = sv.sector and smax.year = sv.year
where max_variation = variation;


-----------------------
-- Usefull for task (c)
-----------------------

-- For each sector, year and ticker -> sum of the volums

create table sector_year_ticker_to_volumeSum as
select d2.sector, year(d1.price_date) as price_year, d1.ticker, sum(d1.volume) as volume
from ${hivevar:DATASET_NAMESIZE} as d1
join historical_stocks_clean as d2 on d1.ticker = d2.ticker
group by d2.sector, year(d1.price_date), d1.ticker;


-- For each sector and year  -> max value of the volume in that year for that sector

create table sector_year_to_maxVolume as
select sector, price_year, max(volume) as maxVolume
from sector_year_ticker_to_volumeSum
group by sector, price_year
order by sector, price_year;


-- For each sector and year -> ticker with the max value of the volume in that year for that sector

create table sector_year_toMaxVolumeTicker as
select ayt.sector, ayt.price_year, ayt.ticker as v_ticker, ayt.volume
from sector_year_ticker_to_volumeSum as ayt
left join sector_year_to_maxVolume as aym on ayt.sector = aym.sector and ayt.price_year = aym.price_year
where volume = maxVolume;


-- Saves the output table in hdfs path /users/hive/warehouse/
-- Create results table
create table job2_hive as
select d2.sector, smin.year, min(((smax.last_quot - smin.first_quot)/smin.first_quot)*100) as variation, max(sy.ticker), max(sy.max_variation), min(v_ticker), max(syv.volume)
from ${hivevar:DATASET_NAMESIZE} as d1
left join historical_stocks_clean as d2 on d1.ticker = d2.ticker
left join sector_to_min_quot as smin on d2.sector = smin.sector and smin.year = extract(year from d1.price_date)
left join sector_to_max_quot as smax on d2.sector = smax.sector and smax.year = extract(year from d1.price_date)
left join sector_year_to_maxTicker sy on d2.sector = sy.sector and sy.year = extract(year from d1.price_date)
left join sector_year_toMaxVolumeTicker as syv on d2.sector = syv.sector and syv.price_year = extract(year from d1.price_date)
where smin.year >=2009 and smin.year <= 2018 and smax.year >=2009 and smax.year <= 2018 and d2.sector != "N/A"
group by d2.sector, smin.year
order by d2.sector, smin.year;


-- Show results
select * from job2_hive;


-- Drop useless tables
drop table sector_2_date;
drop table sector_ticker_min_max;
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

