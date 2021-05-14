-- Drop tables if exist
drop table docs1;


-- Create the input table 1
CREATE TABLE docs1 (ticker STRING, open_price DECIMAL(38,13), close_price DECIMAL(38,13), adj_close DECIMAL(38,13), low DECIMAL(38,13), high DECIMAL(38,13), volume INT, price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


-- Put the dataset into input tables
LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE docs1;


-- Filters the database to show only 2017 data

create table 2017_data as
select ticker, price_date, extract(month from price_date), close_price
from docs1
where extract(year from price_date) = 2017
order by ticker, price_date;
alter table 2017_data change `_c2` month int;


-- For each ticker and month -> First and last price date of that ticker in that month

create table ticker_month_to_max_min_date as
select ticker, month, min(price_date) as min_date, max(price_date) as max_date
from 2017_data
group by ticker, month;


-- For each ticker and month -> the first quotation for that ticker in that month

create table ticker_to_first_month_quotation as
select d.ticker, d.month, d.close_price
from 2017_data as d
left join ticker_month_to_max_min_date as tm on d.ticker = tm.ticker
where price_date = min_date;


-- For each ticker and month -> the last quotation for that ticker in that month

create table ticker_to_last_month_quotation as
select d.ticker, d.month, d.close_price
from 2017_data as d
left join ticker_month_to_max_min_date as tm on d.ticker = tm.ticker
where price_date = max_date;


-- For each ticker and month -> the first and the last quotation for that ticker in that month

create table ticker_to_first_last_month_quotation as
select first.ticker, first.month, first.close_price as first_quotation, last.close_price as last_quotation
from ticker_to_first_month_quotation as first
left join ticker_to_last_month_quotation as last
on first.ticker = last.ticker and first.month = last.month
order by ticker, month;


-- For each ticker and month -> price variation for that ticker in that month

create table ticker_month_to_variation as
select ticker, month, (((last_quotation - first_quotation)/first_quotation)*100) as variation
from ticker_to_first_last_month_quotation
order by ticker, month;


-- For each pair of tickers and month -> price variation for the first ticker and price variation for the second ticker in that month

create table variations_comparison as
select t1.ticker as ticker_1, t2.ticker as ticker_2, t1.month, cast(t1.variation as decimal(10,2)) as variation_1, cast(t2.variation as decimal(10,2)) as variation_2
from ticker_month_to_variation as t1, ticker_month_to_variation as t2
where t1.ticker > t2.ticker and t1.month = t2.month and (abs(t1.variation - t2.variation) <= 1)
order by ticker_1, ticker_2, t1.month;

-- Result table
create table raw_results as
select ticker_1 as t1, ticker_2 as t2,
max(case when month="1" then "GEN:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as gen,
max(case when month="2" then "FEB:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as feb,
max(case when month="3" then "MAR:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as mar,
max(case when month="4" then "APR:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as apr,
max(case when month="5" then "MAG:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as mag,
max(case when month="6" then "GIU:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as giu,
max(case when month="7" then "LUG:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as lug,
max(case when month="8" then "AGO:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as ago,
max(case when month="9" then "SET:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as sep,
max(case when month="10" then "OTT:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as ott,
max(case when month="11" then "NOV:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as nov,
max(case when month="12" then "DIC:"||" "||"("||variation_1||"%"||", "||variation_2||"%"||")" else "" end) as dic
from variations_comparison
group by ticker_1, ticker_2;

-- Show results
select * from raw_results
where (gen!="" and feb!="" and mar!="" and apr!="" and mag!="" and giu!="" and lug!="" and ago!=""
and sep!="" and ott!="" and nov!="" and dic!="");



-- SPECIFICHE

-- Un job in grado di generarele coppie di aziende che si somigliano (sulla base di una soglia scelta a piacere) in termini di variazione
-- percentuale mensile nell’anno 2017 mostrando l’andamento mensile delle due aziende(es.Soglia=1%, coppie: 1:{Apple, Intel}:GEN: Apple
-- +2%, Intel +2,5%, FEB: Apple +3%, Intel +2,7%, MAR: Apple +0,5%, Intel +1,2%, ...; 2:{Amazon, IBM}: GEN: Amazon +1%, IBM +0,5%, FEB:
-- Amazon +0,7%, IBM +0,5%,MAR: Amazon +1,4%, IBM +0,7%,..)


-- Drop useless tables
drop table 2017_data;
drop table ticker_month_to_max_min_date;
drop table ticker_to_first_month_quotation;
drop table ticker_to_last_month_quotation;
drop table ticker_to_first_last_month_quotation;
drop table ticker_month_to_variation;
drop table variations_comparison;;
drop table raw_results;
