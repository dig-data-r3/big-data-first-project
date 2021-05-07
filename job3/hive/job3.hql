-- Drop tables if exist
DROP TABLE docs;

-- Create the input table
CREATE TABLE docs (ticker STRING, open_price DECIMAL(15,13), close_price DECIMAL(15,13), adj_close DECIMAL(15,13), low DECIMAL(15,13), high DECIMAL(15,13), volume INT, price_date DATE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Put the dataset into the input table
LOAD DATA LOCAL INPATH '/home/fregarz/Scrivania/big-data-first-project/input/historical_stock_prices.csv' OVERWRITE INTO TABLE docs;


-- SPECIFICHE

-- Un job in grado di generarele coppie di aziende che si somigliano (sulla base di una soglia scelta a piacere) in terminidi variazione
-- percentuale mensile nell’anno 2017 mostrando l’andamento mensile delle due aziende(es.Soglia=1%, coppie: 1:{Apple, Intel}:GEN: Apple
-- +2%, Intel +2,5%, FEB: Apple +3%, Intel +2,7%, MAR: Apple +0,5%, Intel +1,2%, ...; 2:{Amazon, IBM}: GEN: Amazon +1%, IBM +0,5%, FEB:
-- Amazon +0,7%, IBM +0,5%,MAR: Amazon +1,4%, IBM +0,7%,..)
