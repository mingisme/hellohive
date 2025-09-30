1. Raw Data in HDFS

Example transaction log file (sales.csv):

order_id,customer_id,product_id,amount,timestamp<br>
1001,501,A123,50.0,2025-09-30 10:10:00<br>
1002,502,B234,20.0,2025-09-30 10:20:00<br>
1003,501,A123,50.0,2025-09-30 10:25:00<br>
1004,503,C345,100.0,2025-09-30 10:40:00<br>

2. Define Hive Table

Load the data into Hive (schema-on-read — Hive just maps SQL schema onto files).<br>

CREATE DATABASE retail;<br>
USE retail;<br>

CREATE TABLE sales (<br>
    order_id     BIGINT,<br>
    customer_id  BIGINT,<br>
    product_id   STRING,<br>
    amount       DOUBLE,<br>
    ts           TIMESTAMP<br>
)<br>
ROW FORMAT DELIMITED<br>
FIELDS TERMINATED BY ','<br>
STORED AS TEXTFILE;<br>

3. Load Data

If file is already in HDFS:<br>

LOAD DATA INPATH '/data/sales/sales.csv' INTO TABLE sales;<br>

4. Run Queries (Typical Hive Usage)<br>
a) Total revenue per day<br>
SELECT date(ts) AS day, SUM(amount) AS total_revenue<br>
FROM sales<br>
GROUP BY date(ts)<br>
ORDER BY day;<br>

b) Top 5 selling products<br>
SELECT product_id, SUM(amount) AS total_sales<br>
FROM sales<br>
GROUP BY product_id<br>
ORDER BY total_sales DESC<br>
LIMIT 5;<br>

c) Top 3 customers<br>
SELECT customer_id, SUM(amount) AS total_spent<br>
FROM sales<br>
GROUP BY customer_id<br>
ORDER BY total_spent DESC<br>
LIMIT 3;<br>
