CREATE TABLE historical_stock_prices (
    ticker STRING,
    open_price DECIMAL(38,13),
    close_price DECIMAL(38,13),
    adj_close DECIMAL(38,13),
    low DECIMAL(38,13),
    high DECIMAL(38,13),
    volume INT,
    price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH './dataset/historical_stock_prices.csv'
OVERWRITE INTO TABLE historical_stock_prices;


CREATE TABLE historical_stocks (
    ticker STRING, 
    exch STRING, 
    name STRING, 
    sector STRING, 
    industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH './dataset/historical_stocks.csv'
OVERWRITE INTO TABLE historical_stocks;


-- Cleaned historical stocks file (without null sectors)
CREATE TABLE historical_stocks_clean (
	ticker STRING,
	exch STRING,
	name STRING,
	sector STRING,
	industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH './dataset/historical_stocks_clean.csv'
OVERWRITE INTO TABLE historical_stocks_clean;
