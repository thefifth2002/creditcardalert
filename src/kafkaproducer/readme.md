create a topic with 4 partitions

`any-node:~$ /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic transaction_producer --partitions 4 --replication-factor 2`

start kafka producer from local 

`localhost:~$ bash spawn_kafka_transaction_producer.sh "master node" 8 transaction`

replace "master node" with amazon public DNS
