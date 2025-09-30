# hellohive

## start up hive with docker
docker-compose up -d

## connect to hive
docker exec -it hive-server bash

beeline -u jdbc:hive2://localhost:10000 -n hive

or
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -n hive


## create database
CREATE DATABASE testdb;

USE testdb;

CREATE TABLE users (id INT, name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' INTO TABLE users;

SELECT * FROM users LIMIT 5;

