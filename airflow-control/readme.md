bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
kafka-server-start.sh config/server.properties

#Kafka
./start-cluster.sh

kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic1

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topic1 --partitions 3 --replication-factor 1

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --group ptree --from-beginning

######## spark

pyspark --> kafka --> data_gen.py -->

todo:
topic creation automate using partition
website python server and receive data from gcs
