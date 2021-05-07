-- Drop tables if exist
DROP TABLE docs;
DROP TABLE results;

-- Create the input table
CREATE TABLE docs (ticker STRING, open_price DECIMAL(15,13), close_price DECIMAL(15,13), adj_close DECIMAL(15,13), low DECIMAL(15,13), high DECIMAL(15,13), volume INT, price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Put the dataset into the input table
LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/input/historical_stock_prices.csv' OVERWRITE INTO TABLE docs;

create table high_low_price_values as
    select ticker,
        case
            when open_price > close_price then open_price
            else close_price
        end as greatest_value,
        case
            when open_price < close_price then open_price
            else close_price
        end as lowest_value
    from docs
    order by ticker;


create table close_price_date as
    select distinct ticker, close_price, price_date
    from docs
    where


-- Create the output table
--CREATE TABLE results (ticker STRING, first_price_date DATE, last_price_date DATE, price_variation DECIMAL(5,2), max_price DECIMAL(5,2), min_price DECIMAL(5,2));
create table results as
    select ticker,
        min(price_date) as first_price_date,
        max(price_date) as last_price_date,

        (select close_price from close_price_date where price_date =  last_price_date and close_price_date.ticker docs.ticker)
        - (select close_price from close_price_date where price_date =  first_price_date and close_price_date.ticker docs.ticker),

        max(greatest_value) from high_low_price_values as max_price where high_low_price_values.ticker = results.ticker,
        min(lowest_value) from high_low_price_values as min_price where high_low_price_values.ticker = results.ticker,

    from docs
    group by ticker
    order by last_price_date;

SELECT * FROM results LIMIT 30;

drop table high_low_price_values;
drop table first_last_close_price;