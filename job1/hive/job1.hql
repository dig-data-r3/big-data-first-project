-- Drop tables if exist
DROP TABLE docs;
DROP TABLE results;

-- Create the input table
CREATE TABLE docs (ticker STRING, open_price DECIMAL(15,13), close_price DECIMAL(15,13), adj_close DECIMAL(15,13), low DECIMAL(15,13), high DECIMAL(15,13), volume INT, price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Put the dataset into the input table
LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/input/historical_stock_prices.csv' OVERWRITE INTO TABLE docs;

-- Create the output table
CREATE TABLE results (ticker STRING, first_price_date DATE, last_price_date DATE, price_variation DECIMAL(5,2), max_price DECIMAL(5,2), min_price DECIMAL(5,2));

SELECT * FROM docs LIMIT 20;





