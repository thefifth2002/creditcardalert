# Credit Card Alert
A real time credit card transaction alert project

Copyright ©2017 Hao Wu

# Motivation
* 1 in 10 Americans have at least 1 card.
* 0.04% (4 out of every 10,000) of all credit cards monthly activities were fraudulent.
* This system can also apply to currency and trading monitor, and other platforms involved transactions

# Pipeline
![Image of pipeline](/src/flask/app/static/images/pipeline.png)

* Amazon S3 - store 1M records of customer data.
* Spark Streaming - ingestion of customer data to MySQL database; processing realtime transaction data
* Kafka - simulation of real time transaction data feed to Spark Streaming.
* MySQL database - store cutomer data and accumulating transaction data.
* Flask and bootstrap - enable front-end visulization.

# Detection algorithm
Credit card purchases can be both online and local purchases.
1. If a purchase was made at local store: Compare customer zipcode to local store zipcode from transaction data, if the difference is too big, like > 800, this transaction is maked as "remote local". Customer data has a "travel_alerted" field, if the travel_alerted is "NO", apply algorithm 1; if it is "YES", skip algorithm 1 and apply algorithm 3.
2. If a purchase was made on line: check customer zipcode againse billing address zipcode. If billing zipcode is different, this transaction is marked as "wrong billing". If the billing zipcode is correct, apply algorithm 3.
3. Check customer current balance + purchase amount again credit card limit. If limit is reached, mark transaction as "limit reached", if not, update current balance.

# Repo structure
`/src/flask` Contains all front end files.

`/src/ingestion` Ingest customer data from amazon s3 to MySQL databaes.

`/src/kafkaproducer` Kafkaproduer produce transaction streaming data.

`/src/sparkstreaming` Sparking Streaming process transaction data.

