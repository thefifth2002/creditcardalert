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
