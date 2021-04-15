cd /opt/

wget https://downloads.apache.org/kafka/2.7.0/kafka_2.13-2.7.0.tgz

tar -xvzf kafka_2.13-2.7.0.tgz

cd kafka_2.13-2.7.0

bin/zookeeper-server-start.sh config/zookeeper.properties &

JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties &
